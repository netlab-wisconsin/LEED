
#ifndef _KV_VALUE_LOG_H_
#define _KV_VALUE_LOG_H_
#include "kv_bucket_log.h"
#include "kv_circular_log.h"

#define KV_VALUE_LOG_UNIT_SHIFT (8)
#define KV_VALUE_LOG_UNIT_SIZE (1U << KV_VALUE_LOG_UNIT_SHIFT)
#define KV_VALUE_LOG_UNIT_MASK (KV_VALUE_LOG_UNIT_SIZE - 1)

struct kv_value_log {
    struct kv_circular_log log;
    struct kv_bucket_log *bucket_log;
    uint64_t blk_mask, blk_shift;
    uint64_t compact_head;
    void *bucket_id_log;
    uint64_t id_log_size;
};

static inline uint64_t kv_value_log_offset(struct kv_value_log *self) { return self->log.tail << self->blk_shift; }

// base(bytes) size(bytes)
void kv_value_log_init(struct kv_value_log *self, struct kv_storage *storage, struct kv_bucket_log *bucket_log, uint64_t base,
                       uint64_t size, uint32_t buf_len);
void kv_value_log_fini(struct kv_value_log *self);

// To avoid unnecessary copy, value buffer size is at least value_length + block_size.
void kv_value_log_write(struct kv_value_log *self, uint64_t bucket_id, uint8_t *value, uint32_t value_length,
                        kv_circular_log_io_cb cb, void *cb_arg);
                        
void kv_value_log_read(struct kv_value_log *self, uint64_t offset, uint8_t *value, uint32_t value_length,
                       kv_circular_log_io_cb cb, void *cb_arg);
#endif