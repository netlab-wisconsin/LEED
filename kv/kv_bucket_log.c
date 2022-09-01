#include "kv_bucket_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>
#include <sys/uio.h>

#include "kv_app.h"
#include "kv_circular_log.h"
#include "kv_memory.h"

// --- lock & unlock ---
struct bucket_lock_q_entry {
    struct kv_bucket_lock_entry *bucket_id_set;
    kv_task_cb cb;
    void *cb_arg;
    TAILQ_ENTRY(bucket_lock_q_entry)
    entry;
};
TAILQ_HEAD(bucket_lock_q_head, bucket_lock_q_entry);
void kv_bucket_lock_add(struct kv_bucket_lock_entry **set, uint64_t bucket_id) {
    struct kv_bucket_lock_entry *entry;
    HASH_FIND(hh, *set, &bucket_id, sizeof(uint64_t), entry);
    if (entry == NULL) {
        entry = kv_malloc(sizeof(struct kv_bucket_lock_entry));
        entry->bucket_id = bucket_id;
        entry->ref_cnt = 1;
        HASH_ADD(hh, *set, bucket_id, sizeof(uint64_t), entry);
    } else {
        entry->ref_cnt++;
    }
}
static inline bool lockable(struct kv_bucket_log *self, struct kv_bucket_lock_entry *set) {
    for (struct kv_bucket_lock_entry *entry = set; entry != NULL; entry = entry->hh.next)
        if (kv_bucket_get_meta(self, entry->bucket_id)->lock) return false;
    return true;
}
static inline void lock_flip(struct kv_bucket_log *self, struct kv_bucket_lock_entry *set) {
    for (struct kv_bucket_lock_entry *entry = set; entry != NULL; entry = entry->hh.next)
        kv_bucket_get_meta(self, entry->bucket_id)->lock = ~kv_bucket_get_meta(self, entry->bucket_id)->lock;
}

void kv_bucket_lock(struct kv_bucket_log *self, struct kv_bucket_lock_entry *set, kv_task_cb cb, void *cb_arg) {
    if (lockable(self, set)) {
        lock_flip(self, set);
        if (cb) kv_app_send(self->log.thread_index, cb, cb_arg);
    } else {
        struct bucket_lock_q_entry *q_entry = kv_malloc(sizeof(struct bucket_lock_q_entry));
        q_entry->cb = cb;
        q_entry->cb_arg = cb_arg;
        q_entry->bucket_id_set = set;
        TAILQ_INSERT_TAIL((struct bucket_lock_q_head *)self->waiting_queue, q_entry, entry);
    }
}
void kv_bucket_unlock(struct kv_bucket_log *self, struct kv_bucket_lock_entry **set) {
    struct bucket_lock_q_head *queue = self->waiting_queue;
    lock_flip(self, *set);
    struct kv_bucket_lock_entry *x, *y, *tmp;
    HASH_ITER(hh, *set, x, tmp) {
        struct bucket_lock_q_entry *q_entry;
        TAILQ_FOREACH(q_entry, queue, entry) {
            HASH_FIND(hh, q_entry->bucket_id_set, &x->bucket_id, sizeof(uint64_t), y);
            if (y && lockable(self, q_entry->bucket_id_set)) break;
        }
        if (q_entry) {
            lock_flip(self, q_entry->bucket_id_set);
            if (q_entry->cb) kv_app_send(self->log.thread_index, q_entry->cb, q_entry->cb_arg);
            TAILQ_REMOVE(queue, q_entry, entry);
            kv_free(q_entry);
        }
        HASH_DEL(*set, x);
        kv_free(x);
    }
}

// --- compact ---
#define COMPACTION_CONCURRENCY 16
#define COMPACTION_LENGTH 128
static void compact_move_head(struct kv_bucket_log *self) {
    struct kv_bucket *bucket;
    uint32_t i;
    for (i = 0; (self->log.size - self->compact_head + self->log.head + i) % self->log.size; i += bucket->chain_length) {
        uint32_t bucket_offset = (self->log.head + i) % self->log.size;
        kv_circular_log_fetch_one(&self->log, bucket_offset, (void **)&bucket);
        if (kv_bucket_get_meta(self, bucket->id)->bucket_offset == bucket_offset) break;
    }
    self->head = (self->head + i) % self->size;
    kv_circular_log_move_head(&self->log, i);
}

struct compact_ctx {
    struct kv_bucket_log *self;
    struct kv_bucket_lock_entry *bucket_id_set;
    uint32_t compact_head, len;
    struct kv_bucket_segments segments;
};
static void compact_lock_cb(void *arg);
static void compact_write_cb(bool success, void *arg) {
    if (!success) {
        fprintf(stderr, "compact_write_cb: IO error, retrying ...");
        compact_lock_cb(arg);
        return;
    }
    struct compact_ctx *ctx = arg;
    struct kv_bucket_log *self = ctx->self;
    struct kv_bucket_segment *seg;
    while ((seg = TAILQ_FIRST(&ctx->segments)) != NULL) {
        TAILQ_REMOVE(&ctx->segments, seg, entry);
        kv_bucket_seg_commit(self, seg);
        kv_bucket_seg_cleanup(self, seg);
    }
    kv_bucket_unlock(self, &ctx->bucket_id_set);
    compact_move_head(self);
    kv_free(arg);
}

static void compact_lock_cb(void *arg) {
    struct compact_ctx *ctx = arg;
    struct kv_bucket_log *self = ctx->self;
    struct iovec iov[2];
    struct kv_bucket_lock_entry *unlock_set = NULL, *x, *tmp;
    HASH_ITER(hh, ctx->bucket_id_set, x, tmp) {
        struct kv_bucket_meta *meta = kv_bucket_get_meta(self, x->bucket_id);
        if ((self->log.size - ctx->compact_head + meta->bucket_offset) % self->log.size < ctx->len && meta->chain_length != 0) {
            kv_circular_log_fetch(&self->log, meta->bucket_offset, meta->chain_length, iov);
            struct kv_bucket_segment *seg = kv_malloc(sizeof(*seg));
            TAILQ_INIT(&seg->chain);
            seg->bucket_id = x->bucket_id;
            for (size_t i = 0; i < 2 && iov[i].iov_len != 0; i++) {
                struct kv_bucket_chain_entry *chain_entry = kv_malloc(sizeof(*chain_entry));
                chain_entry->len = iov[i].iov_len;
                chain_entry->bucket = iov[i].iov_base;
                chain_entry->pre_alloc_bucket = true;
                TAILQ_INSERT_TAIL(&seg->chain, chain_entry, entry);
            }
            TAILQ_INSERT_TAIL(&ctx->segments, seg, entry);
        } else {
            HASH_DEL(ctx->bucket_id_set, x);
            HASH_ADD(hh, unlock_set, bucket_id, sizeof(uint64_t), x);
        }
    }
    kv_bucket_unlock(self, &unlock_set);
    kv_bucket_seg_put_bulk(self, &ctx->segments, compact_write_cb, ctx);
}

static void compact(struct kv_bucket_log *self) {
    if (kv_circular_log_empty_space(&self->log) >= COMPACTION_LENGTH * COMPACTION_CONCURRENCY * 6) return;
    if ((self->log.size - self->log.head + self->compact_head) % self->log.size > COMPACTION_LENGTH * COMPACTION_CONCURRENCY) return;
    struct compact_ctx *ctx = kv_malloc(sizeof(struct compact_ctx));
    ctx->self = self;
    ctx->bucket_id_set = NULL;
    ctx->compact_head = self->compact_head;
    TAILQ_INIT(&ctx->segments);

    struct kv_bucket *bucket;
    for (ctx->len = 0; ctx->len < COMPACTION_LENGTH; ctx->len += bucket->chain_length) {
        uint32_t bucket_offset = (self->compact_head + ctx->len) % self->log.size;
        kv_circular_log_fetch_one(&self->log, bucket_offset, (void **)&bucket);
        assert(bucket->chain_index == 0 && bucket->chain_length);

        struct kv_bucket_meta *meta = kv_bucket_get_meta(self, bucket->id);
        if (meta->chain_length != 0 && meta->bucket_offset == bucket_offset)
            kv_bucket_lock_add(&ctx->bucket_id_set, bucket->id);
    }
    self->compact_head = (self->compact_head + ctx->len) % self->log.size;
    kv_bucket_lock(self, ctx->bucket_id_set, compact_lock_cb, ctx);
}

// --- init & fini ---
void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets,
                        kv_circular_log_io_cb cb, void *cb_arg) {
    num_buckets = num_buckets > COMPACTION_LENGTH << 4 ? num_buckets : COMPACTION_LENGTH << 4;
    assert(storage->block_size == sizeof(struct kv_bucket));
    kv_memset(self, 0, sizeof(struct kv_bucket_log));
    kv_circular_log_init(&self->log, storage, base, num_buckets << 1, 0, 0, COMPACTION_LENGTH * COMPACTION_CONCURRENCY * 4, 256);

    uint64_t log_num_buckets = 31;
    while ((1U << log_num_buckets) > num_buckets) log_num_buckets--;
    self->bucket_num = 1U << log_num_buckets;
    self->size = self->log.size << 1;

    self->bucket_meta = kv_calloc(self->bucket_num, sizeof(struct kv_bucket_meta));
    kv_memset(self->bucket_meta, 0, sizeof(struct kv_bucket_meta) * self->bucket_num);

    self->waiting_queue = kv_malloc(sizeof(struct bucket_lock_q_head));
    TAILQ_INIT((struct bucket_lock_q_head *)self->waiting_queue);
    self->init = false;
    if (cb) cb(true, cb_arg);
}

void kv_bucket_log_fini(struct kv_bucket_log *self) {
    kv_free(self->bucket_meta);
    kv_free(self->waiting_queue);
    kv_circular_log_fini(&self->log);
}

// --- writev ---
static void kv_bucket_log_writev(struct kv_bucket_log *self, struct iovec *buckets, int iovcnt, kv_circular_log_io_cb cb,
                                 void *cb_arg) {
    for (int i = 0; i < iovcnt; i++) {
        for (size_t j = 0; j < buckets[i].iov_len; j++) {
            struct kv_bucket *bucket = (struct kv_bucket *)buckets[i].iov_base + j;
            self->tail = (self->tail + 1) % self->size;
            bucket->head = self->head;
            bucket->tail = self->tail;
        }
    }
    kv_circular_log_appendv(&self->log, buckets, iovcnt, cb, cb_arg);
    if (!self->init) compact(self);
}

// bucket alloc & free
bool kv_bucket_alloc_extra(struct kv_bucket_log *self, struct kv_bucket_segment *seg) {
    uint8_t length = TAILQ_EMPTY(&seg->chain) ? 0 : TAILQ_FIRST(&seg->chain)->bucket->chain_length;
    if (length == 0x7F) return false;
    struct kv_bucket_chain_entry *ce = kv_malloc(sizeof(*ce));
    ce->len = 1;
    ce->bucket = kv_storage_zblk_alloc(self->log.storage, ce->len);
    ce->pre_alloc_bucket = false;
    TAILQ_INSERT_TAIL(&seg->chain, ce, entry);

    ce->bucket->id = seg->bucket_id;
    ce->bucket->chain_index = length;
    length++;
    TAILQ_FOREACH(ce, &seg->chain, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket) {
            bucket->chain_length = length;
        }
    }
    return true;
}

void kv_bucket_free_extra(struct kv_bucket_segment *seg) {
    assert(!TAILQ_EMPTY(&seg->chain));
    uint8_t length = TAILQ_FIRST(&seg->chain)->bucket->chain_length;
    struct kv_bucket_chain_entry *ce = TAILQ_LAST(&seg->chain, kv_bucket_chain);
    if (ce->len > 1) {
        ce->len--;
    } else {
        TAILQ_REMOVE(&seg->chain, ce, entry);
        if (!ce->pre_alloc_bucket)
            kv_storage_free(ce->bucket);
        kv_free(ce);
    }
    length--;
    TAILQ_FOREACH(ce, &seg->chain, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket) {
            bucket->chain_length = length;
        }
    }
}
// --- debug ---
static inline void verify_buckets(struct kv_bucket_log *self, struct kv_bucket_segment *seg) {
#ifndef NDEBUG
    uint64_t bucket_id = KV_BUCKET_ID_EMPTY, len, i = 0;
    struct kv_bucket_chain_entry *ce;
    TAILQ_FOREACH(ce, &seg->chain, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket) {
            if (bucket_id == KV_BUCKET_ID_EMPTY) {
                bucket_id = bucket->id;
                assert(bucket_id < self->bucket_num);
                len = bucket->chain_length;
            } else {
                assert(bucket->chain_index == i);
                assert(bucket->chain_length == len);
                assert(bucket->id == bucket_id);
            }
            ++i;
        }
    }
    assert(len == i);
#endif
}
// --- bucket segment ---
void kv_bucket_seg_get(struct kv_bucket_log *self, uint64_t bucket_id, struct kv_bucket_segment *seg, kv_circular_log_io_cb cb, void *cb_arg) {
    struct kv_bucket_meta *meta = kv_bucket_get_meta(self, bucket_id);
    TAILQ_INIT(&seg->chain);
    seg->bucket_id = bucket_id;
    if (meta->chain_length != 0) {
        struct kv_bucket_chain_entry *chain_entry = kv_malloc(sizeof(*chain_entry));
        chain_entry->len = meta->chain_length;
        chain_entry->bucket = kv_storage_blk_alloc(self->log.storage, chain_entry->len);
        chain_entry->pre_alloc_bucket = false;
        TAILQ_INSERT_HEAD(&seg->chain, chain_entry, entry);
        kv_circular_log_read(&self->log, meta->bucket_offset, chain_entry->bucket, meta->chain_length, cb, cb_arg);
    } else {
        if (cb) cb(true, cb_arg);
    }
}

static inline void iov_append(struct iovec *iov, uint32_t *iov_i, void *iov_base, size_t iov_len) {
    if (*iov_i == 0 || (struct kv_bucket *)iov[*iov_i - 1].iov_base + iov[*iov_i - 1].iov_len != iov_base)
        iov[(*iov_i)++] = (struct iovec){iov_base, iov_len};
    else
        iov[*iov_i - 1].iov_len += iov_len;
}

void kv_bucket_seg_put_bulk(struct kv_bucket_log *self, struct kv_bucket_segments *segs, kv_circular_log_io_cb cb, void *cb_arg) {
    struct kv_bucket_segment *seg;
    struct kv_bucket_chain_entry *chain_entry;
    int iovcnt = 0;
    TAILQ_FOREACH(seg, segs, entry) {
        TAILQ_FOREACH(chain_entry, &seg->chain, entry) {
            iovcnt++;
        }
    }
    if (iovcnt == 0) {
        if (cb) cb(true, cb_arg);
        return;
    }
    struct iovec buckets[iovcnt];
    uint32_t len = 0, iov_i = 0;
    TAILQ_FOREACH(seg, segs, entry) {
        verify_buckets(self, seg);
        seg->offset = (self->log.tail + len) % self->log.size;
        len += TAILQ_FIRST(&seg->chain)->bucket->chain_length;
        TAILQ_FOREACH(chain_entry, &seg->chain, entry) {
            iov_append(buckets, &iov_i, chain_entry->bucket, chain_entry->len);
        }
    }
    kv_bucket_log_writev(self, buckets, iov_i, cb, cb_arg);
}

void kv_bucket_seg_put(struct kv_bucket_log *self, struct kv_bucket_segment *seg, kv_circular_log_io_cb cb, void *cb_arg) {
    struct kv_bucket_segments segs;
    TAILQ_INIT(&segs);
    TAILQ_INSERT_HEAD(&segs, seg, entry);
    kv_bucket_seg_put_bulk(self, &segs, cb, cb_arg);
}

void kv_bucket_seg_cleanup(struct kv_bucket_log *self, struct kv_bucket_segment *seg) {
    struct kv_bucket_chain_entry *chain_entry;
    while ((chain_entry = TAILQ_FIRST(&seg->chain)) != NULL) {
        TAILQ_REMOVE(&seg->chain, chain_entry, entry);
        if (!chain_entry->pre_alloc_bucket)
            kv_storage_free(chain_entry->bucket);
        kv_free(chain_entry);
    }
}

static bool is_empty_seg(struct kv_bucket_segment *seg) {
    if (TAILQ_EMPTY(&seg->chain)) return true;
    struct kv_bucket *first_bucket = TAILQ_FIRST(&seg->chain)->bucket;
    if (first_bucket->chain_length > 1) return false;
    for (struct kv_item *item = first_bucket->items; item - first_bucket->items < KV_ITEM_PER_BUCKET; ++item)
        if (!KV_EMPTY_ITEM(item))
            return false;
    return true;
}

void kv_bucket_seg_commit(struct kv_bucket_log *self, struct kv_bucket_segment *seg) {
    struct kv_bucket_chain_entry *chain_entry;
    TAILQ_FOREACH(chain_entry, &seg->chain, entry) {
        verify_buckets(self, seg);
    }

    struct kv_bucket_meta *meta = kv_bucket_get_meta(self, seg->bucket_id);
    if (is_empty_seg(seg)) {
        meta->chain_length = 0;
    } else {
        meta->bucket_offset = seg->offset;
        meta->chain_length = TAILQ_FIRST(&seg->chain)->bucket->chain_length;
    }
}