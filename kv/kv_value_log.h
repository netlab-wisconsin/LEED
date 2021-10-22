
#ifndef _KV_VALUE_LOG_H_
#define _KV_VALUE_LOG_H_
#include "kv_storage.h"
struct kv_value_log {
    uint64_t head, tail, blk_mask,base,size;
    bool compact_at_write;
    struct kv_storage *storage;
};

typedef kv_storage_io_cb kv_value_log_io_cb;
//base(bytes) size(bytes)
void kv_value_log_init(struct kv_value_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head, uint64_t tail);
void kv_value_log_fini(struct kv_value_log *self);
// To avoid unnecessary copy, value buffer size is at least value_length+block_size.
#define KV_VALUE_LOG_ERROR_OFFSET UINT64_MAX
uint64_t kv_value_log_write(struct kv_value_log *self, uint8_t *value, uint32_t value_length, kv_value_log_io_cb cb,
                            void *cb_arg);
//TODO: write vector
void kv_value_log_read(struct kv_value_log *self, uint64_t offset, uint8_t *value, uint32_t value_length, kv_value_log_io_cb cb,
                       void *cb_arg);

#endif