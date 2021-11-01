
#ifndef _KV_WAITING_MAP_H_
#define _KV_WAITING_MAP_H_
#include <stdint.h>
typedef void (*kv_task_cb)(void *);
struct kv_waiting_task {
    kv_task_cb cb;
    void *cb_arg;
};

void kv_waiting_map_put(void **map, uint32_t index, kv_task_cb cb, void *cb_arg);
struct kv_waiting_task kv_waiting_map_get(void **map, uint32_t index);
static inline void kv_waiting_queue_put(void **queue, kv_task_cb cb, void *cb_arg) { kv_waiting_map_put(queue, 0, cb, cb_arg); }
static inline struct kv_waiting_task kv_waiting_queue_get(void **queue) { return kv_waiting_map_get(queue, 0); }
#endif