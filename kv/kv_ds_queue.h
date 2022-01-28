#ifndef _KV_DS_QUEUE_
#define _KV_DS_QUEUE_
#include <stdatomic.h>
#include <stdint.h>

struct kv_ds_q_info {
    uint32_t size;
    uint32_t cap;
};
typedef _Atomic struct kv_ds_q_info kv_ds_atomic_q;

struct kv_ds_queue {
    uint32_t ds_cnt;
    kv_ds_atomic_q *q_info;
    atomic_uint *io_cnt;
};
#define KV_DS_Q_NOTFOUND UINT32_MAX
void kv_ds_queue_init(struct kv_ds_queue *self, uint32_t ds_cnt);
void kv_ds_queue_fini(struct kv_ds_queue *self);
uint32_t kv_ds_queue_find(struct kv_ds_q_info *qs, uint32_t *io_cnt, uint32_t size);
#endif