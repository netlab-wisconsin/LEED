
#ifndef _KV_VALUE_LOG_H_
#define _KV_VALUE_LOG_H_
#include "kv_bucket_log.h"
#include "kv_circular_log.h"
#define KV_INDEX_LOG_ENTRY_BIT (7)
#define KV_INDEX_LOG_ENTRY_PER_BLOCK (1U << KV_INDEX_LOG_ENTRY_BIT)
#define KV_INDEX_LOG_ENTRY_MASK (KV_INDEX_LOG_ENTRY_PER_BLOCK - 1)

#define KV_VALUE_LOG_UNIT_SHIFT (8)
#define KV_VALUE_LOG_UNIT_SIZE (1U << KV_VALUE_LOG_UNIT_SHIFT)
#define KV_VALUE_LOG_UNIT_MASK (KV_VALUE_LOG_UNIT_SIZE - 1)
struct kv_value_log_compact {
    uint8_t *val_buf, *val_buf_head;
    uint32_t *index_buf;
    struct iovec *iov;
    uint32_t iovcnt;
    void *bucket_map, *current_bucket;
    uint64_t val_buf_len;
    uint64_t index_blk_num;
    uint32_t lock_cnt;
    struct {
        uint32_t err : 1;
        uint32_t running : 1;
        uint32_t io_cnt : 30;
    } state;
};

struct kv_value_log {
    struct kv_circular_log log;
    struct kv_circular_log index_log;
    struct kv_bucket_log *bucket_log;
    uint64_t head, blk_mask, blk_shift;
    uint32_t *append_buf[2], buf_len;
    uint8_t append_buf_i;
    bool index_log_dump_running;
    struct kv_value_log_compact compact;
};

static inline uint64_t kv_value_log_offset(struct kv_value_log *self) { return self->log.tail << self->blk_shift; }

// base(bytes) size(bytes)
void kv_value_log_init(struct kv_value_log *self, struct kv_storage *storage, struct kv_bucket_log *bucket_log, uint64_t base,
                       uint64_t size, uint32_t buf_len);
void kv_value_log_fini(struct kv_value_log *self);

// To avoid unnecessary copy, value buffer size is at least value_length + block_size.
void kv_value_log_write(struct kv_value_log *self, int32_t bucket_index, uint8_t *value, uint32_t value_length,
                        kv_circular_log_io_cb cb, void *cb_arg);
// TODO: write vector
void kv_value_log_read(struct kv_value_log *self, uint64_t offset, uint8_t *value, uint32_t value_length,
                       kv_circular_log_io_cb cb, void *cb_arg);
#endif