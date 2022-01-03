#include "kv_app.h"

#include <pthread.h>
#include <stdio.h>

#include "concurrentqueue.h"
#include "memery_operation.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/thread.h"

pthread_mutex_t g_lock;
static struct kv_app_t g_app;
static struct spdk_mempool *g_event_mempool;
static struct thread_data {
    struct spdk_thread *thread;
    MoodycamelCQHandle cq;
    MoodycamelToken c_token, p_tokens[MAX_TASKS_NUM];
    struct spdk_poller *poller;
} g_threads[MAX_TASKS_NUM];

const struct kv_app_t *kv_app(void) { return &g_app; }

void kv_app_send_msg(uint32_t index, kv_app_func func, void *arg) { spdk_thread_send_msg(g_threads[index].thread, func, arg); }

uint32_t kv_app_get_thread_index(void) {
    const struct spdk_thread *thread = spdk_get_thread();
    uint32_t index;
    for (index = 0; g_threads[index].thread != thread; ++index)
        ;
    return index;
}

void kv_app_send(uint32_t index, kv_app_func func, void *arg) {
    uint32_t local_index = kv_app_get_thread_index();
    struct kv_app_task *msg = spdk_mempool_get(g_event_mempool);
    *msg = (struct kv_app_task){func, arg};
    moodycamel_cq_enqueue_with_token(g_threads[index].cq, g_threads[index].p_tokens[local_index], msg);
}

#define MAX_POLL_SZ 16
static int msg_poller(void *arg) {
    struct thread_data *data = arg;
    struct kv_app_task *msg[MAX_POLL_SZ];
    size_t size;
    while ((size = moodycamel_cq_try_dequeue_bulk_with_token(data->cq, data->c_token, (MoodycamelValue *)msg, MAX_POLL_SZ))) {
        for (size_t i = 0; i < size; i++) {
            if (msg[i]->func) msg[i]->func(msg[i]->arg);
            // kv_free(msg[i]);
            spdk_mempool_put(g_event_mempool, msg[i]);
        }
    }
    return 0;
}

static void app_start(void *arg) {
    struct kv_app_task *tasks = arg;
    for (uint32_t i = 0; i < g_app.task_num; ++i)
        if (tasks[i].func) kv_app_send(i, tasks[i].func, tasks[i].arg);
}
static inline void thread_init(uint32_t index) {
    g_threads[index].thread = spdk_get_thread();
    moodycamel_cq_create(&g_threads[index].cq);
    for (size_t i = 0; i < g_app.task_num; i++) moodycamel_prod_token(g_threads[index].cq, &g_threads[index].p_tokens[i]);
    moodycamel_cons_token(g_threads[index].cq, &g_threads[index].c_token);
    g_threads[index].poller = spdk_poller_register(msg_poller, g_threads + index, 0);
}

static void register_func(void *arg) {
    struct spdk_thread *thread = spdk_get_thread();
    puts(spdk_thread_get_name(thread));
    uint32_t index;
    if (sscanf(spdk_thread_get_name(thread), "reactor_%u", &index) == 1) {
        thread_init(index);
    }
}
static void send_msg_to_all(void *arg) {
    char mempool_name[64];
    snprintf(mempool_name, sizeof(mempool_name), "kv_app_evtpool_%d", getpid());
    g_event_mempool = spdk_mempool_create(mempool_name, 262144 - 1, /* Power of 2 minus 1 is optimal for memory consumption */
                                          sizeof(struct kv_app_task), SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_SOCKET_ID_ANY);
    thread_init(0);
    spdk_for_each_thread(register_func, arg, app_start);
}

void *kv_app_poller_register(kv_app_poller_func func, void *arg, uint64_t period_microseconds) {
    return spdk_poller_register(func, arg, period_microseconds);
}
struct poller_register_ctx {
    kv_app_poller_func func;
    void *arg;
    uint64_t period_microseconds;
    void **poller;
};
static void poller_register(void *arg) {
    struct poller_register_ctx *ctx = arg;
    *ctx->poller = spdk_poller_register(ctx->func, ctx->arg, ctx->period_microseconds);
    kv_free(arg);
}
void kv_app_poller_register_on(uint32_t index, kv_app_poller_func func, void *arg, uint64_t period_microseconds,
                               void **poller) {
    struct poller_register_ctx *ctx = kv_malloc(sizeof(*ctx));
    *ctx = (struct poller_register_ctx){func, arg, period_microseconds, poller};
    kv_app_send(index, poller_register, ctx);
}

void kv_app_poller_unregister(void **poller) { spdk_poller_unregister((struct spdk_poller **)poller); }

int kv_app_start(const char *json_config_file, uint32_t task_num, struct kv_app_task *tasks) {
    assert(task_num >= 1 && task_num < MAX_TASKS_NUM);
    struct spdk_app_opts opts;
    static char cpu_mask[255];
    g_app.task_num = task_num;
    g_app.running_thread = (1ULL << task_num) - 1;
    spdk_app_opts_init(&opts);
    sprintf(cpu_mask, "0x%lX", g_app.running_thread);
    opts.name = "kv_app";
    opts.reactor_mask = cpu_mask;
    opts.json_config_file = json_config_file;
    pthread_mutex_init(&g_lock, NULL);
    int rc = 0;
    if ((rc = spdk_app_start(&opts, send_msg_to_all, tasks))) SPDK_ERRLOG("ERROR starting application\n");
    for (size_t i = 0; i < task_num; i++) {
        moodycamel_cons_token_destroy(g_threads[i].c_token);
        for (size_t j = 0; j < g_app.task_num; j++) moodycamel_prod_token_destroy(g_threads[i].p_tokens[j]);
        moodycamel_cq_destroy(g_threads[i].cq);
    }
    spdk_app_fini();
    return rc;
}

void kv_app_stop(int rc) {
    uint32_t index = kv_app_get_thread_index();
    spdk_poller_unregister(&g_threads[index].poller);
    pthread_mutex_lock(&g_lock);
    if (g_app.running_thread) {
        if (rc) {
            spdk_app_stop(rc);
            g_app.running_thread = 0;
        } else {
            g_app.running_thread &= ~(1ULL << index);
            if (!g_app.running_thread) spdk_app_stop(rc);
        }
    }
    pthread_mutex_unlock(&g_lock);
}