#ifndef _KV_BUCKET_LOG_H_
#define _KV_BUCKET_LOG_H_
#include "kv_circular_log.h"
#include "kv_waiting_map.h"
#define KV_MAX_KEY_LENGTH 20
#define KV_MIN_KEY_LENGTH 8
#define KV_ITEM_PER_BUCKET 16

struct kv_item {
    uint8_t key_length;
    uint8_t key[KV_MAX_KEY_LENGTH];
    uint32_t value_length;
    uint64_t value_offset : 48;
#define KV_EMPTY_ITEM(item) (!(item)->key_length)
} __attribute__((packed));

struct kv_bucket {
    uint32_t index;
    uint8_t chain_length, chain_index;
    uint16_t reserved;
    uint32_t head, tail;
    struct kv_item items[KV_ITEM_PER_BUCKET];
};
struct kv_bucket_meta {
    uint8_t lock : 1;
    uint8_t chain_length : 7;
    uint32_t bucket_offset;
} __attribute__((packed));

struct kv_bucket_log_compact {
    struct kv_bucket *buffer[2];
    struct iovec *iov;
    uint32_t iovcnt;
    uint32_t offset, length, buf_len, i;
    struct {
        uint32_t err : 1;
        uint32_t running : 1;
        uint32_t io_cnt : 30;
    } state;
};

struct kv_bucket_log {
    struct kv_circular_log log;
    uint32_t size;
    uint32_t head, tail;
    struct kv_bucket_meta *bucket_meta;
    uint32_t bucket_num;
    void *waiting_map;
    struct kv_bucket_log_compact compact;
};

static inline void kv_bucket_log_move_head(struct kv_bucket_log *self, uint32_t n) {
    self->head = (self->head + n) % self->size;
    self->log.head = (self->log.head + n) % self->log.size;
}

static inline uint32_t kv_bucket_log_offset(struct kv_bucket_log *self) { return (uint32_t)self->log.tail; }

static inline struct kv_bucket_meta *kv_bucket_get_meta(struct kv_bucket_log *self, uint32_t index) {
    return self->bucket_meta + index;
}

void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint64_t base, uint32_t size,
                        uint32_t log_num_buckets, uint32_t compact_buf_len, kv_circular_log_io_cb cb, void *cb_arg);
void kv_bucket_log_fini(struct kv_bucket_log *self);

static inline void kv_bucket_log_read(struct kv_bucket_log *self, uint32_t offset, struct kv_bucket *buckets, uint32_t n,
                                      kv_circular_log_io_cb cb, void *cb_arg) {
    kv_circular_log_read(&self->log, offset, buckets, n, cb, cb_arg);
}
void kv_bucket_log_writev(struct kv_bucket_log *self, struct iovec *buckets, int iovcnt, kv_circular_log_io_cb cb,
                          void *cb_arg);
static inline void kv_bucket_log_write(struct kv_bucket_log *self, struct kv_bucket *buckets, uint32_t n,
                                       kv_circular_log_io_cb cb, void *cb_arg) {
    struct iovec iov = {.iov_base = buckets, .iov_len = n};
    kv_bucket_log_writev(self, &iov, 1, cb, cb_arg);
}

void kv_bucket_lock(struct kv_bucket_log *self, uint32_t index, kv_task_cb cb, void *cb_arg);
void kv_bucket_unlock(struct kv_bucket_log *self, uint32_t index);
#endif