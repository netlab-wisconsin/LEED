#ifndef _KV_APP_H_
#define _KV_APP_H_
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
typedef void (*kv_app_func)(void *ctx);
struct kv_app_t {
    uint32_t task_num;
    uint64_t running_thread;
    uint64_t *thread_ids;
};
struct kv_app_task {
    kv_app_func func;
    void *arg;
};
struct kv_app_t kv_app(void);
int kv_app_start(const char *json_config_file, uint32_t task_num, struct kv_app_task *tasks);
static inline int kv_app_start_single_task(const char *json_config_file, kv_app_func func, void *arg){
    struct kv_app_task task = {func, arg};
    return kv_app_start(json_config_file, 1, &task);
}
void kv_app_stop(int rc);
void kv_app_send_msg(uint32_t index, kv_app_func func, void *arg);
uint32_t kv_app_get_index(void);
#endif