#include "kv_app.h"

#include <stdio.h>

#include "memery_operation.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/thread.h"

static struct kv_app_t g_app;
const struct kv_app_t * kv_app(void) {
    return &g_app;
}

void kv_app_send_msg(const void *thread, kv_app_func func, void *arg) {
    spdk_thread_send_msg((const struct spdk_thread *)thread, func, arg);
}
void *kv_app_get_thread(void) { return spdk_get_thread(); }

static void app_start(void *arg) {
    struct kv_app_task *tasks = arg;
    g_app.threads[0] = spdk_get_thread();    
    for (uint32_t i = 0; i < g_app.task_num; ++i){
        if(i==0) g_app.threads[i] = spdk_get_thread();
        else g_app.threads[i] = spdk_thread_create(NULL,NULL);
        if (tasks[i].func) kv_app_send_msg(g_app.threads[i], tasks[i].func, tasks[i].arg);
    }
}

int kv_app_start(const char *json_config_file, uint32_t task_num, struct kv_app_task *tasks) {
    assert(task_num >= 1 && task_num < sizeof(uint64_t));
    struct spdk_app_opts opts;
    static char cpu_mask[255];
    g_app.task_num = task_num;
    g_app.threads = kv_calloc(task_num, sizeof(void *));
    g_app.running_thread = (1ULL << task_num) - 1;
    spdk_app_opts_init(&opts);
    sprintf(cpu_mask, "0x%lX", g_app.running_thread);
    opts.name = "kv_app";
    opts.reactor_mask = cpu_mask;
    opts.json_config_file = json_config_file;
    int rc = 0;
    if ((rc = spdk_app_start(&opts, app_start, tasks))) SPDK_ERRLOG("ERROR starting application\n");
    spdk_app_fini();
    kv_free(g_app.threads);
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
    kv_app_send_msg(g_app.threads[0], app_stop, arg);
}