#ifndef _KV_BUCKET_LOG_H_
#define _KV_BUCKET_LOG_H_
#include "kv_circular_log.h"
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
    uint32_t next_extra_bucket_index;
    uint32_t head, tail;
    struct kv_item items[KV_ITEM_PER_BUCKET];
};

struct kv_bucket_log {
    struct kv_circular_log log;
    uint32_t size;
    uint32_t head, tail;
};
static inline void kv_bucket_log_move_head(struct kv_bucket_log *self, uint32_t n) {
    self->head = (self->head + n) % self->size;
    self->log.head = (self->log.head + n) % self->log.size;
}
void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint32_t base, uint32_t size, uint32_t head,
                        uint32_t tail);
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
void kv_bucket_log_fini(struct kv_bucket_log *self);
#endif