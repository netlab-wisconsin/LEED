#ifndef _KV_BUCKET_LOG_H_
#define _KV_BUCKET_LOG_H_
#include <sys/queue.h>

#include "kv_circular_log.h"

#define KV_MAX_KEY_LENGTH 20
#define KV_MIN_KEY_LENGTH 8
#define KV_ITEM_PER_BUCKET 16

typedef void (*kv_task_cb)(void *);

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
    uint8_t chain_length;
    uint32_t bucket_offset;
} __attribute__((packed));

struct kv_bucket_chain_entry {
    struct kv_bucket *bucket;
    uint8_t len;
    bool pre_alloc_bucket;
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
    uint32_t compact_head;
    void *meta, *bucket_lock;
};

static inline uint32_t kv_bucket_log_offset(struct kv_bucket_log *self) { return (uint32_t)self->log.tail; }

void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets);
void kv_bucket_log_fini(struct kv_bucket_log *self);

bool kv_bucket_alloc_extra(struct kv_bucket_log *self, struct kv_bucket_segment *seg);
void kv_bucket_free_extra(struct kv_bucket_segment *seg);

void kv_bucket_seg_get(struct kv_bucket_log *self, uint64_t bucket_id, struct kv_bucket_segment *seg, kv_circular_log_io_cb cb, void *cb_arg);
void kv_bucket_seg_put(struct kv_bucket_log *self, struct kv_bucket_segment *seg, kv_circular_log_io_cb cb, void *cb_arg);
void kv_bucket_seg_put_bulk(struct kv_bucket_log *self, struct kv_bucket_segments *segs, kv_circular_log_io_cb cb, void *cb_arg);
void kv_bucket_seg_cleanup(struct kv_bucket_log *self, struct kv_bucket_segment *seg);
void kv_bucket_seg_commit(struct kv_bucket_log *self, struct kv_bucket_segment *seg);

void kv_bucket_meta_init(struct kv_bucket_log *self);
void kv_bucket_meta_fini(struct kv_bucket_log *self);
struct kv_bucket_meta kv_bucket_meta_get(struct kv_bucket_log *self, uint64_t bucket_id);
void kv_bucket_meta_put(struct kv_bucket_log *self, uint64_t bucket_id, struct kv_bucket_meta data);

void kv_bucket_lock(struct kv_bucket_log *self, struct kv_bucket_segments *segs, kv_task_cb cb, void *cb_arg);
void kv_bucket_unlock(struct kv_bucket_log *self, struct kv_bucket_segments *segs);
void kv_bucket_lock_init(struct kv_bucket_log *self);
void kv_bucket_lock_fini(struct kv_bucket_log *self);
#endif