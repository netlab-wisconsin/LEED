#include "kv_bucket_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>
#include <sys/uio.h>

#include "kv_app.h"
#include "kv_circular_log.h"
#include "kv_memory.h"

// --- compact ---
#define COMPACTION_CONCURRENCY 4
#define COMPACTION_LENGTH 512
static void compact_move_head(struct kv_bucket_log *self) {
    struct kv_bucket *bucket;
    uint32_t i;
    for (i = 0; (self->log.size - self->compact_head + self->log.head + i) % self->log.size; i += bucket->chain_length) {
        uint32_t bucket_offset = (self->log.head + i) % self->log.size;
        kv_circular_log_fetch_one(&self->log, bucket_offset, (void **)&bucket);
        struct kv_bucket_meta meta = kv_bucket_meta_get(self, bucket->id);
        if (meta.chain_length != 0 && meta.bucket_offset == bucket_offset) break;
    }
    self->head = (self->head + i) % self->size;
    kv_circular_log_move_head(&self->log, i);
}

struct compact_ctx {
    struct kv_bucket_log *self;
    kv_bucket_lock_set bucket_id_set;
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
        kv_free(seg);
    }
    kv_bucket_unlock(self, &ctx->bucket_id_set);
    compact_move_head(self);
    kv_free(arg);
}

#define TAILQ_FOREACH_SAFE(var, head, field, tvar) \
    for ((var) = TAILQ_FIRST((head)); (var) && ((tvar) = TAILQ_NEXT((var), field), 1); (var) = (tvar))

static void compact_lock_cb(void *arg) {
    struct compact_ctx *ctx = arg;
    struct kv_bucket_log *self = ctx->self;
    kv_bucket_lock_set unlock_set = NULL;
    struct kv_bucket_segment *seg, *tmp;
    TAILQ_FOREACH_SAFE(seg, &ctx->segments, entry, tmp) {
        struct kv_bucket_meta meta = kv_bucket_meta_get(self, seg->bucket_id);
        if ((self->log.size - ctx->compact_head + meta.bucket_offset) % self->log.size >= ctx->len || meta.chain_length == 0) {
            TAILQ_REMOVE(&ctx->segments, seg, entry);
            kv_bucket_lock_set_add(&unlock_set, seg->bucket_id);
            kv_bucket_lock_set_del(&ctx->bucket_id_set, seg->bucket_id);
            kv_bucket_seg_cleanup(self, seg);
            kv_free(seg);
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

        struct kv_bucket_meta meta = kv_bucket_meta_get(self, bucket->id);
        if (meta.chain_length != 0 && meta.bucket_offset == bucket_offset) {
            struct iovec iov[2];
            kv_circular_log_fetch(&self->log, meta.bucket_offset, meta.chain_length, iov);
            struct kv_bucket_segment *seg = kv_malloc(sizeof(*seg));
            TAILQ_INIT(&seg->chain);
            seg->bucket_id = bucket->id;
            for (size_t i = 0; i < 2 && iov[i].iov_len != 0; i++) {
                struct kv_bucket_chain_entry *chain_entry = kv_malloc(sizeof(*chain_entry));
                chain_entry->len = iov[i].iov_len;
                chain_entry->bucket = iov[i].iov_base;
                chain_entry->pre_alloc_bucket = true;
                TAILQ_INSERT_TAIL(&seg->chain, chain_entry, entry);
            }
            TAILQ_INSERT_TAIL(&ctx->segments, seg, entry);
            kv_bucket_lock_set_add(&ctx->bucket_id_set, bucket->id);
        }
    }
    self->compact_head = (self->compact_head + ctx->len) % self->log.size;
    kv_bucket_lock(self, ctx->bucket_id_set, compact_lock_cb, ctx);
}

// --- init & fini ---
void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets) {
    num_buckets = num_buckets > COMPACTION_LENGTH << 4 ? num_buckets : COMPACTION_LENGTH << 4;
    assert(storage->block_size == sizeof(struct kv_bucket));
    kv_memset(self, 0, sizeof(struct kv_bucket_log));

    self->log_bucket_num = 31;
    while ((1U << self->log_bucket_num) >= num_buckets) self->log_bucket_num--;
    self->bucket_num = 1U << ++self->log_bucket_num;
    kv_circular_log_init(&self->log, storage, base, num_buckets << 1, 0, 0, COMPACTION_LENGTH * COMPACTION_CONCURRENCY * 4, 256);
    self->size = self->log.size << 1;
    kv_bucket_meta_init(self);
    kv_bucket_lock_init(self);
}

void kv_bucket_log_fini(struct kv_bucket_log *self) {
    kv_bucket_meta_fini(self);
    kv_bucket_lock_fini(self);
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
    compact(self);
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
    struct kv_bucket_meta meta = kv_bucket_meta_get(self, bucket_id);
    TAILQ_INIT(&seg->chain);
    seg->bucket_id = bucket_id;
    if (meta.chain_length != 0) {
        struct kv_bucket_chain_entry *chain_entry = kv_malloc(sizeof(*chain_entry));
        chain_entry->len = meta.chain_length;
        chain_entry->bucket = kv_storage_blk_alloc(self->log.storage, chain_entry->len);
        chain_entry->pre_alloc_bucket = false;
        TAILQ_INSERT_HEAD(&seg->chain, chain_entry, entry);
        kv_circular_log_read(&self->log, meta.bucket_offset, chain_entry->bucket, meta.chain_length, cb, cb_arg);
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

    if (is_empty_seg(seg)) {
        kv_bucket_meta_put(self, seg->bucket_id, (struct kv_bucket_meta){0, 0});
    } else {
        struct kv_bucket_meta meta = {TAILQ_FIRST(&seg->chain)->bucket->chain_length, seg->offset};
        kv_bucket_meta_put(self, seg->bucket_id, meta);
    }
}