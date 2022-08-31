#ifndef _KV_BUCKET_LOG_H_
#define _KV_BUCKET_LOG_H_
#include <sys/queue.h>

#include "kv_circular_log.h"
#include "utils/uthash.h"

#define KV_MAX_KEY_LENGTH 20
#define KV_MIN_KEY_LENGTH 8
#define KV_ITEM_PER_BUCKET 16

typedef void (*kv_task_cb)(void *);

struct kv_bucket_lock_entry {
    uint64_t bucket_id;
    uint32_t ref_cnt;
    UT_hash_handle hh;
};

struct kv_item {
    uint8_t key_length;
    uint8_t key[KV_MAX_KEY_LENGTH];
    uint32_t value_length;
    uint64_t value_offset : 48;
#define KV_EMPTY_ITEM(item) (!(item)->key_length)
} __attribute__((packed));

#define KV_BUCKET_ID_EMPTY (0xFFFFFFFFFFFFu)
struct kv_bucket {
    uint64_t id : 48;
    uint8_t chain_length, chain_index;
    uint32_t head, tail;
    struct kv_item items[KV_ITEM_PER_BUCKET];
};
struct kv_bucket_meta {
    uint8_t lock : 1;
    uint8_t chain_length : 7;
    uint32_t bucket_offset;
} __attribute__((packed));

struct kv_bucket_chain_entry {
    struct kv_bucket *bucket;
    uint8_t len;
    TAILQ_ENTRY(kv_bucket_chain_entry)
    entry;
};
TAILQ_HEAD(kv_bucket_chain, kv_bucket_chain_entry);

struct kv_bucket_segment {
    uint64_t bucket_id;
    TAILQ_HEAD(, kv_bucket_chain_entry)
    chain;
    uint32_t offset;
    TAILQ_ENTRY(kv_bucket_segment)
    entry;
};
TAILQ_HEAD(kv_bucket_segments, kv_bucket_segment);

struct kv_bucket_log {
    struct kv_circular_log log;
    uint32_t size;
    uint32_t head, tail;
    uint32_t compact_head, compact_len;
    struct kv_bucket_meta *bucket_meta;
    uint64_t bucket_num;
    void *waiting_queue;
    bool init;
};

static inline uint32_t kv_bucket_log_offset(struct kv_bucket_log *self) { return (uint32_t)self->log.tail; }

static inline struct kv_bucket_meta *kv_bucket_get_meta(struct kv_bucket_log *self, uint64_t bucket_id) {
    return self->bucket_meta + bucket_id;
}

void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets,
                        uint32_t compact_buf_len, kv_circular_log_io_cb cb, void *cb_arg);
void kv_bucket_log_fini(struct kv_bucket_log *self);

void kv_bucket_log_writev(struct kv_bucket_log *self, struct iovec *buckets, int iovcnt, kv_circular_log_io_cb cb,
                          void *cb_arg);
static inline void kv_bucket_log_write(struct kv_bucket_log *self, struct kv_bucket *buckets, uint32_t n,
                                       kv_circular_log_io_cb cb, void *cb_arg) {
    struct iovec iov = {.iov_base = buckets, .iov_len = n};
    kv_bucket_log_writev(self, &iov, 1, cb, cb_arg);
}

bool kv_bucket_alloc_extra(struct kv_bucket_log *self, struct kv_bucket_segment *seg);
void kv_bucket_free_extra(struct kv_bucket_segment *seg);

void kv_bucket_seg_get(struct kv_bucket_log *self, uint64_t bucket_id, struct kv_bucket_segment *seg, kv_circular_log_io_cb cb, void *cb_arg);
void kv_bucket_seg_put(struct kv_bucket_log *self, struct kv_bucket_segment *seg, kv_circular_log_io_cb cb, void *cb_arg);
void kv_bucket_seg_put_bulk(struct kv_bucket_log *self, struct kv_bucket_segments *segs, kv_circular_log_io_cb cb, void *cb_arg);
void kv_bucket_seg_cleanup(struct kv_bucket_log *self, struct kv_bucket_segment *seg);
void kv_bucket_seg_commit(struct kv_bucket_log *self, struct kv_bucket_segment *seg);

void kv_bucket_lock_add(struct kv_bucket_lock_entry **set, uint64_t bucket_id);
void kv_bucket_lock(struct kv_bucket_log *self, struct kv_bucket_lock_entry *set, kv_task_cb cb, void *cb_arg);
void kv_bucket_unlock(struct kv_bucket_log *self, struct kv_bucket_lock_entry **set);
#endif