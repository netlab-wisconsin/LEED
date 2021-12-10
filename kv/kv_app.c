#include "kv_app.h"

#include <pthread.h>
#include <stdio.h>
#include <sys/queue.h>

#include "memery_operation.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/thread.h"

static struct kv_app_t g_app;
struct msg_entry {
    kv_app_func func;
    void *arg;
    STAILQ_ENTRY(msg_entry) next;
};

struct thread_data {
    pthread_mutex_t lock;
    STAILQ_HEAD(, msg_entry) msgs;
    struct spdk_poller *poller;
} g_threads[MAX_TASKS_NUM];

const struct kv_app_t *kv_app(void) { return &g_app; }

void kv_app_send(uint32_t index, kv_app_func func, void *arg) {
    struct msg_entry *msg = kv_malloc(sizeof(struct msg_entry));
    *msg = (struct msg_entry){func, arg};
    pthread_mutex_lock(&g_threads[index].lock);
    STAILQ_INSERT_TAIL(&g_threads[index].msgs, msg, next);
    pthread_mutex_unlock(&g_threads[index].lock);
}

static int msg_poller(void *arg) {
    struct thread_data *data = arg;
    while (true) {
        pthread_mutex_lock(&data->lock);
        if (STAILQ_EMPTY(&data->msgs)) {
            pthread_mutex_unlock(&data->lock);
            break;
        }
        struct msg_entry *msg = STAILQ_FIRST(&data->msgs);
        STAILQ_REMOVE_HEAD(&data->msgs, next);
        pthread_mutex_unlock(&data->lock);
        if (msg->func) msg->func(msg->arg);
        kv_free(msg);
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
        g_threads[index].poller = spdk_poller_register(msg_poller, g_threads + index, 0);
    }
}
static void send_msg_to_all(void *arg) {
    g_app.threads[0] = spdk_get_thread();
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
    for (size_t i = 0; i < task_num; i++) {
        pthread_mutex_init(&g_threads[i].lock, NULL);
        STAILQ_INIT(&g_threads[i].msgs);
    }
    int rc = 0;
    if ((rc = spdk_app_start(&opts, send_msg_to_all, tasks))) SPDK_ERRLOG("ERROR starting application\n");
    spdk_app_fini();
    return rc;
}

struct app_stop_arg {
    uint32_t index;
    int rc;
};

static void app_stop(void *_arg) {
    struct app_stop_arg *arg = _arg;
    if (g_app.running_thread) {
        if (arg->rc) {
            spdk_app_stop(arg->rc);
            g_app.running_thread = 0;
        } else {
            g_app.running_thread &= ~(1ULL << arg->index);
            if (!g_app.running_thread) spdk_app_stop(arg->rc);
        }
    }
    kv_free(arg);
}
void kv_app_stop(int rc) {
    struct app_stop_arg *arg = kv_malloc(sizeof(struct app_stop_arg));
    const struct spdk_thread *thread = spdk_get_thread();
    for (arg->index = 0; g_app.threads[arg->index] != thread; ++arg->index)
        ;
    arg->rc = rc;
    spdk_poller_unregister(&g_threads[arg->index].poller);
    kv_app_send(0, app_stop, arg);
}