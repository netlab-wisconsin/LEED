
#ifndef _KV_VALUE_LOG_H_
#define _KV_VALUE_LOG_H_
#include "kv_circular_log.h"
struct kv_value_log {
    struct kv_circular_log log;
    uint64_t head, blk_mask, blk_shift;
};

static inline uint64_t kv_value_log_offset(struct kv_value_log *self) { return self->log.tail << self->blk_shift; }

// base(bytes) size(bytes)
void kv_value_log_init(struct kv_value_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                       uint64_t tail);
void kv_value_log_fini(struct kv_value_log *self);

// To avoid unnecessary copy, value buffer size is at least value_length+block_size.
void kv_value_log_write(struct kv_value_log *self, uint8_t *value, uint32_t value_length, kv_circular_log_io_cb cb,
                        void *cb_arg);
// TODO: write vector
void kv_value_log_read(struct kv_value_log *self, uint64_t offset, uint8_t *value, uint32_t value_length,
                       kv_circular_log_io_cb cb, void *cb_arg);
#endif