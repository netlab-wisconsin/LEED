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

struct thread_data {
    MoodycamelCQHandle cq;
    struct spdk_poller *poller;
} g_threads[MAX_TASKS_NUM];

const struct kv_app_t *kv_app(void) { return &g_app; }

void kv_app_send(uint32_t index, kv_app_func func, void *arg) {
    struct kv_app_task *msg = kv_malloc(sizeof(struct kv_app_task));
    *msg = (struct kv_app_task){func, arg};
    moodycamel_cq_enqueue(g_threads[index].cq, msg);
}

#define MAX_POLL_SZ 16
static int msg_poller(void *arg) {
    struct thread_data *data = arg;
    struct kv_app_task *msg[MAX_POLL_SZ];
    size_t size;
    while ((size = moodycamel_cq_try_dequeue_bulk(data->cq, (MoodycamelValue *)msg, MAX_POLL_SZ))) {
        for (size_t i = 0; i < size; i++) {
            if (msg[i]->func) msg[i]->func(msg[i]->arg);
            kv_free(msg[i]);
        }
    }
    return 0;
}

void kv_app_send_msg(const void *thread, kv_app_func func, void *arg) {
    spdk_thread_send_msg((const struct spdk_thread *)thread, func, arg);
}
void *kv_app_get_thread(void) { return spdk_get_thread(); }

static void app_start(void *arg) {
    struct kv_app_task *tasks = arg;
    for (uint32_t i = 0; i < g_app.task_num; ++i)
        if (tasks[i].func) kv_app_send(i, tasks[i].func, tasks[i].arg);
}
static void register_func(void *arg) {
    struct spdk_thread *thread = spdk_get_thread();
    puts(spdk_thread_get_name(thread));
    uint32_t index;
    if (sscanf(spdk_thread_get_name(thread), "reactor_%u", &index) == 1) {
        g_app.threads[index] = thread;
        moodycamel_cq_create(&g_threads[index].cq);
        g_threads[index].poller = spdk_poller_register(msg_poller, g_threads + index, 0);
    }
}
static void send_msg_to_all(void *arg) {
    g_app.threads[0] = spdk_get_thread();
    moodycamel_cq_create(&g_threads[0].cq);
    g_threads[0].poller = spdk_poller_register(msg_poller, g_threads, 0);
    spdk_for_each_thread(register_func, arg, app_start);
}

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
    for (size_t i = 0; i < task_num; i++) moodycamel_cq_destroy(g_threads[i].cq);
    spdk_app_fini();
    return rc;
}

void kv_app_stop(int rc) {
    const struct spdk_thread *thread = spdk_get_thread();
    uint32_t index;
    for (index = 0; g_app.threads[index] != thread; ++index)
        ;
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