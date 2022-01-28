#include "kv_ds_queue.h"

#include <assert.h>

#include "kv_memory.h"
void kv_ds_queue_init(struct kv_ds_queue *self, uint32_t ds_cnt) {
    self->ds_cnt = ds_cnt;
    self->q_info = kv_malloc(sizeof(*self->q_info));
    self->io_cnt = kv_malloc(sizeof(*self->io_cnt));
}
void kv_ds_queue_fini(struct kv_ds_queue *self) {
    kv_free(self->q_info);
    kv_free(self->io_cnt);
}
uint32_t kv_ds_queue_find(struct kv_ds_q_info *qs, uint32_t *io_cnt, uint32_t size) {
    assert(size);
    uint32_t j = 0;
    for (size_t i = 0; i < size; i++) {
        if (io_cnt && io_cnt[i] == 0) return i;
        if (qs[i].cap - qs[i].size > qs[j].cap - qs[j].size) j = i;
    }
    if (qs[j].cap == qs[j].size) return KV_DS_Q_NOTFOUND;
    return j;
}