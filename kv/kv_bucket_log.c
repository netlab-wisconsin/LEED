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
    struct kv_bucket_lock_entry *index_set;
    kv_task_cb cb;
    void *cb_arg;
    TAILQ_ENTRY(bucket_lock_q_entry) entry;
};
TAILQ_HEAD(bucket_lock_q_head, bucket_lock_q_entry);
void kv_bucket_lock_add_index(struct kv_bucket_lock_entry **set, uint32_t index) {
    struct kv_bucket_lock_entry *entry;
    HASH_FIND_INT(*set, &index, entry);
    if (entry == NULL) {
        entry = kv_malloc(sizeof(struct kv_bucket_lock_entry));
        entry->index = index;
        entry->ref_cnt = 1;
        HASH_ADD_INT(*set, index, entry);
    } else {
        entry->ref_cnt++;
    }
}
static inline bool lockable(struct kv_bucket_log *self, struct kv_bucket_lock_entry *set) {
    for (struct kv_bucket_lock_entry *entry = set; entry != NULL; entry = entry->hh.next)
        if (self->bucket_meta[entry->index].lock) return false;
    return true;
}
static inline void lock_flip(struct kv_bucket_log *self, struct kv_bucket_lock_entry *set) {
    for (struct kv_bucket_lock_entry *entry = set; entry != NULL; entry = entry->hh.next)
        self->bucket_meta[entry->index].lock = ~self->bucket_meta[entry->index].lock;
}

void kv_bucket_lock(struct kv_bucket_log *self, struct kv_bucket_lock_entry *set, kv_task_cb cb, void *cb_arg) {
    if (lockable(self, set)) {
        lock_flip(self, set);
        if (cb) kv_app_send(self->log.thread_index, cb, cb_arg);
    } else {
        struct bucket_lock_q_entry *q_entry = kv_malloc(sizeof(struct bucket_lock_q_entry));
        q_entry->cb = cb;
        q_entry->cb_arg = cb_arg;
        q_entry->index_set = set;
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
            HASH_FIND_INT(q_entry->index_set, &x->index, y);
            if (y && lockable(self, q_entry->index_set)) break;
        }
        if (q_entry) {
            lock_flip(self, q_entry->index_set);
            if (q_entry->cb) kv_app_send(self->log.thread_index, q_entry->cb, q_entry->cb_arg);
            TAILQ_REMOVE(queue, q_entry, entry);
            kv_free(q_entry);
        }
        HASH_DEL(*set, x);
        kv_free(x);
    }
}

// --- compact ---
#define COMPACT_CIO_NUM 4
static void compact_move_head(struct kv_bucket_log *self) {
    struct kv_bucket *bucket;
    uint32_t i;
    for (i = 0; (self->log.size - self->compact_head + self->log.head + i) % self->log.size; i += bucket->chain_length) {
        uint32_t bucket_offset = (self->log.head + i) % self->log.size;
        kv_circular_log_fetch_one(&self->log, bucket_offset, (void **)&bucket);
        if (self->bucket_meta[bucket->index].bucket_offset == bucket_offset) break;
    }
    self->head = (self->head + i) % self->size;
    kv_circular_log_move_head(&self->log, i);
}

struct compact_ctx {
    struct kv_bucket_log *self;
    struct kv_bucket_lock_entry *index_set;
    uint32_t compact_head, len;
    uint32_t offset;
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
    for (struct kv_bucket_lock_entry *entry = ctx->index_set; entry != NULL; entry = entry->hh.next) {
        struct kv_bucket_meta *meta = self->bucket_meta + entry->index;
        meta->bucket_offset = ctx->offset;
        ctx->offset = (ctx->offset + meta->chain_length) % self->log.size;
    }
    kv_bucket_unlock(self, &ctx->index_set);
    compact_move_head(self);
    kv_free(arg);
}

static inline void append_iov(struct iovec *iov0, uint32_t *cnt, struct iovec iov[2]) {
    if ((*cnt) == 0 || (struct kv_bucket *)iov0[*cnt - 1].iov_base + iov0[*cnt - 1].iov_len != iov[0].iov_base)
        iov0[(*cnt)++] = iov[0];
    else
        iov0[*cnt - 1].iov_len += iov[0].iov_len;
    if (iov[1].iov_len) iov0[(*cnt)++] = iov[1];
}

static void compact_lock_cb(void *arg) {
    struct compact_ctx *ctx = arg;
    struct kv_bucket_log *self = ctx->self;
    uint32_t iovcnt = 0;
    struct iovec *iov0 = kv_calloc(HASH_COUNT(ctx->index_set), sizeof(struct iovec));
    struct iovec iov[2];
    struct kv_bucket_lock_entry *unlock_set = NULL, *x, *tmp;
    HASH_ITER(hh, ctx->index_set, x, tmp) {
        struct kv_bucket_meta *meta = self->bucket_meta + x->index;
        if ((self->log.size - ctx->compact_head + meta->bucket_offset) % self->log.size < ctx->len) {
            kv_circular_log_fetch(&self->log, meta->bucket_offset, meta->chain_length, iov);
            append_iov(iov0, &iovcnt, iov);
        } else {
            HASH_DEL(ctx->index_set, x);
            HASH_ADD_INT(unlock_set, index, x);
        }
    }
    kv_bucket_unlock(self, &unlock_set);
    ctx->offset = kv_bucket_log_offset(self);
    if (iovcnt)
        kv_bucket_log_writev(self, iov0, iovcnt, compact_write_cb, ctx);
    else
        compact_write_cb(true, ctx);
    kv_free(iov0);
}

static void compact(struct kv_bucket_log *self) {
    if (kv_circular_log_empty_space(&self->log) >= self->compact_len * COMPACT_CIO_NUM * 4) return;
    if ((self->log.size - self->log.head + self->compact_head) % self->log.size > self->compact_len * COMPACT_CIO_NUM) return;
    struct compact_ctx *ctx = kv_malloc(sizeof(struct compact_ctx));
    ctx->self = self;
    ctx->index_set = NULL;
    ctx->compact_head = self->compact_head;

    struct kv_bucket *bucket;
    for (ctx->len = 0; ctx->len < self->compact_len; ctx->len += bucket->chain_length) {
        uint32_t bucket_offset = (self->compact_head + ctx->len) % self->log.size;
        kv_circular_log_fetch_one(&self->log, bucket_offset, (void **)&bucket);
        assert(bucket->chain_index == 0 && bucket->chain_length);
        if (self->bucket_meta[bucket->index].bucket_offset == bucket_offset)
            kv_bucket_lock_add_index(&ctx->index_set, bucket->index);
    }
    self->compact_head = (self->compact_head + ctx->len) % self->log.size;
    kv_bucket_lock(self, ctx->index_set, compact_lock_cb, ctx);
}

// --- init & fini ---
#define INIT_CON_IO_NUM 32

struct init_ctx {
    struct kv_bucket_log *self;
    kv_circular_log_io_cb cb;
    void *cb_arg;
    uint32_t io_num;
    bool success;
};

struct init_write_ctx {
    struct init_ctx *init_ctx;
    struct kv_bucket buckets[0];
};

static void init(struct kv_bucket_log *self, kv_circular_log_io_cb cb, void *cb_arg) {
    for (size_t i = 0; i < self->log.fetch.size - 1; i++) ((struct kv_bucket *)self->log.fetch.buffer)[i].index = i;
    self->bucket_meta = kv_calloc(self->bucket_num, sizeof(struct kv_bucket_meta));
    kv_memset(self->bucket_meta, 0, sizeof(struct kv_bucket_meta) * self->bucket_num);
    for (size_t i = 0; i < self->bucket_num; i++) {
        self->bucket_meta[i].bucket_offset = self->log.head + i;
        self->bucket_meta[i].chain_length = 1;
    }
    self->waiting_queue = kv_malloc(sizeof(struct bucket_lock_q_head));
    TAILQ_INIT((struct bucket_lock_q_head *)self->waiting_queue);
    self->mem_pool = kv_mempool_create(20000, sizeof(struct kv_bucket));

    self->init = false;
    if (cb) cb(true, cb_arg);
}

static void init_write_cb(bool success, void *arg) {
    struct init_write_ctx *ctx = arg;
    struct kv_bucket_log *self = ctx->init_ctx->self;
    if (!(ctx->init_ctx->success = ctx->init_ctx->success && success)) {
        if (--ctx->init_ctx->io_num == 0) {
            if (ctx->init_ctx->cb) ctx->init_ctx->cb(false, ctx->init_ctx->cb_arg);
            kv_free(ctx->init_ctx);
        }
        kv_storage_free(ctx);
        return;
    }
    uint32_t n = self->log.size - self->tail - 1;
    uint32_t head = self->log.size - 1 - self->bucket_num;
    if (n == 0) {
        if (--ctx->init_ctx->io_num == 0) {
            init(self, ctx->init_ctx->cb, ctx->init_ctx->cb_arg);
            kv_free(ctx->init_ctx);
        }
        kv_storage_free(ctx);
        return;
    }
    if (n <= self->compact_len) {
        self->head = head;
        self->log.head = head;
        self->compact_head = head;
    } else
        n = self->compact_len;
    for (size_t i = 0; i < n; i++) {
        if (self->tail + i < head)
            ctx->buckets[i].index = UINT32_MAX;
        else
            ctx->buckets[i].index = self->tail + i - head;
        ctx->buckets[i].chain_length = 1;
    }
    kv_bucket_log_write(self, ctx->buckets, n, init_write_cb, ctx);
}

void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint64_t base, uint32_t num_buckets,
                        uint32_t compact_buf_len, kv_circular_log_io_cb cb, void *cb_arg) {
    assert(storage->block_size == sizeof(struct kv_bucket));
    kv_memset(self, 0, sizeof(struct kv_bucket_log));
    kv_circular_log_init(&self->log, storage, base, num_buckets << 1, 0, 0, compact_buf_len * COMPACT_CIO_NUM * 4, 256);

    uint64_t log_num_buckets = 31;
    while ((1U << log_num_buckets) > num_buckets) log_num_buckets--;
    self->bucket_num = 1U << log_num_buckets;
    self->size = self->log.size << 1;
    self->compact_len = compact_buf_len;
    self->init = true;  // disable compact while init

    struct init_ctx *ctx = kv_malloc(sizeof(struct init_ctx));
    ctx->self = self;
    ctx->cb = cb;
    ctx->cb_arg = cb_arg;
    ctx->io_num = INIT_CON_IO_NUM;
    ctx->success = true;
    for (size_t i = 0; i < ctx->io_num; i++) {
        struct init_write_ctx *buf =
            kv_storage_zmalloc(storage, sizeof(struct init_write_ctx) + compact_buf_len * sizeof(struct kv_bucket));
        buf->init_ctx = ctx;
        init_write_cb(true, buf);
    }
}

void kv_bucket_log_fini(struct kv_bucket_log *self) {
    kv_free(self->bucket_meta);
    kv_free(self->waiting_queue);
    kv_mempool_free(self->mem_pool);
    kv_circular_log_fini(&self->log);
}

// --- writev ---
void kv_bucket_log_writev(struct kv_bucket_log *self, struct iovec *buckets, int iovcnt, kv_circular_log_io_cb cb,
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
bool kv_bucket_alloc_extra(struct kv_bucket_pool *entry) {
    uint8_t length = TAILQ_FIRST(&entry->buckets)->bucket->chain_length;
    if (length == 0x7F) return false;
    struct kv_bucket_chain_entry *ce = kv_malloc(sizeof(*ce));
    ce->len = 1;
    ce->bucket = kv_mempool_get(entry->self->mem_pool);
    TAILQ_INSERT_TAIL(&entry->buckets, ce, entry);
    for (struct kv_item *item = ce->bucket->items; item - ce->bucket->items < KV_ITEM_PER_BUCKET; ++item) item->key_length = 0;

    ce->bucket->index = TAILQ_FIRST(&entry->buckets)->bucket->index;
    ce->bucket->chain_index = length;
    length++;
    TAILQ_FOREACH(ce, &entry->buckets, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket) {
            bucket->chain_length = length;
        }
    }
    return true;
}

void kv_bucket_free_extra(struct kv_bucket_pool *entry) {
    uint8_t length = TAILQ_FIRST(&entry->buckets)->bucket->chain_length;
    assert(length > 1);
    struct kv_bucket_chain_entry *ce = TAILQ_LAST(&entry->buckets, kv_bucket_chain);
    if (ce->len > 1) {
        assert(false);
        ce->len--;
    } else {
        TAILQ_REMOVE(&entry->buckets, ce, entry);
        kv_mempool_put(entry->self->mem_pool, ce->bucket);
        kv_free(ce);
    }
    length--;
    TAILQ_FOREACH(ce, &entry->buckets, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket) {
            bucket->chain_length = length;
        }
    }
}
// --- debug ---
static inline void verify_buckets(struct kv_bucket_pool *entry) {
#ifndef NDEBUG
    uint32_t index = UINT32_MAX, len, i = 0;
    struct kv_bucket_chain_entry *ce;
    TAILQ_FOREACH(ce, &entry->buckets, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket) {
            if (index == UINT32_MAX) {
                index = bucket->index;
                assert(index < entry->self->bucket_num);
                len = bucket->chain_length;
            } else {
                assert(bucket->chain_index == i);
                assert(bucket->chain_length == len);
                assert(bucket->index == index);
            }
            ++i;
        }
    }
    assert(len == i);
#endif
}
// --- bucket_pool ---
static void free_pool_entry(struct kv_bucket_log *self, struct kv_bucket_pool *entry) {
    HASH_DEL(self->pool, entry);
    assert(STAILQ_EMPTY(&entry->get_q));
    struct kv_bucket_chain_entry *chain_entry;
    while ((chain_entry = TAILQ_FIRST(&entry->buckets)) != NULL) {
        TAILQ_REMOVE(&entry->buckets, chain_entry, entry);
        kv_mempool_put(self->mem_pool, chain_entry->bucket);
        kv_free(chain_entry);
    }
    kv_free(entry);
}

static void pool_read_cb(bool success, void *arg) {
    struct kv_bucket_pool *entry = arg;
    verify_buckets(entry);
    entry->is_valid = true;
    struct kv_bucket_pool_get_q *wq;
    while ((wq = STAILQ_FIRST(&entry->get_q)) != NULL) {
        STAILQ_REMOVE_HEAD(&entry->get_q, next);
        if (wq->cb) wq->cb(success ? entry : NULL, wq->cb_arg);
        kv_free(wq);
    }
    if (!success) free_pool_entry(entry->self, entry);
}

void kv_bucket_pool_get(struct kv_bucket_log *self, uint32_t index, kv_bucket_pool_get_cb cb, void *cb_arg) {
    struct kv_bucket_pool *entry;
    HASH_FIND_INT(self->pool, &index, entry);
    if (entry) {
        entry->ref_cnt++;
        if (entry->is_valid) {
            if (cb) cb(entry, cb_arg);
        } else {
            struct kv_bucket_pool_get_q *wq = kv_malloc(sizeof(struct kv_bucket_pool_get_q));
            *wq = (struct kv_bucket_pool_get_q){cb, cb_arg};
            STAILQ_INSERT_TAIL(&entry->get_q, wq, next);
        }
    } else {  // read from bucket log
        entry = kv_malloc(sizeof(struct kv_bucket_pool));
        *entry = (struct kv_bucket_pool){.self = self, .index = index, .is_valid = false, .ref_cnt = 1};
        uint32_t chain_length = self->bucket_meta[index].chain_length;
        struct iovec *iov = kv_calloc(chain_length, sizeof(struct iovec));
        TAILQ_INIT(&entry->buckets);
        for (size_t i = 0; i < chain_length; i++) {
            struct kv_bucket_chain_entry *chain_entry = kv_malloc(sizeof(*chain_entry));
            chain_entry->len = 1;
            chain_entry->bucket = kv_mempool_get(self->mem_pool);
            TAILQ_INSERT_TAIL(&entry->buckets, chain_entry, entry);
            iov[i] = (struct iovec){.iov_base = chain_entry->bucket, .iov_len = chain_entry->len};
        }

        STAILQ_INIT(&entry->get_q);
        struct kv_bucket_pool_get_q *wq = kv_malloc(sizeof(struct kv_bucket_pool_get_q));
        *wq = (struct kv_bucket_pool_get_q){cb, cb_arg};
        STAILQ_INSERT_TAIL(&entry->get_q, wq, next);
        HASH_ADD_INT(self->pool, index, entry);

        kv_circular_log_readv(&self->log, self->bucket_meta[index].bucket_offset, iov, chain_length, pool_read_cb, entry);
        kv_free(iov);
    }
}

#define STAILQ_FOREACH_SAFE(var, head, field, tvar) \
    for ((var) = STAILQ_FIRST((head)); (var) && ((tvar) = STAILQ_NEXT((var), field), 1); (var) = (tvar))

struct pool_put_ctx {
    struct kv_bucket_log *self;
    kv_circular_log_io_cb cb;
    void *cb_arg;
    struct kv_bucket_pool_head pool_head;
};
static void pool_write_cb(bool success, void *arg) {
    struct pool_put_ctx *ctx = arg;
    struct kv_bucket_log *self = ctx->self;
    struct kv_bucket_pool *entry, *tmp;
    if (success) {
        STAILQ_FOREACH(entry, &ctx->pool_head, next) {
            verify_buckets(entry);
            struct kv_bucket *bucket = TAILQ_FIRST(&entry->buckets)->bucket;
            self->bucket_meta[bucket->index].bucket_offset = entry->offset;
            self->bucket_meta[bucket->index].chain_length = bucket->chain_length;
        }
    }
    STAILQ_FOREACH_SAFE(entry, &ctx->pool_head, next, tmp) {
        if (--entry->ref_cnt == 0) free_pool_entry(self, entry);
    }
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx);
}

void kv_bucket_pool_put_bulk(struct kv_bucket_log *self, struct kv_bucket_pool_head *pool_head, kv_circular_log_io_cb cb,
                             void *cb_arg) {
    struct pool_put_ctx *ctx = kv_malloc(sizeof(struct pool_put_ctx));
    *ctx = (struct pool_put_ctx){self, cb, cb_arg};
    STAILQ_INIT(&ctx->pool_head);
    int iovcnt = 0;
    struct kv_bucket_pool *entry, *tmp;
    struct kv_bucket_chain_entry *chain_entry;
    STAILQ_FOREACH_SAFE(entry, pool_head, next, tmp) {
        assert(!TAILQ_EMPTY(&entry->buckets));
        TAILQ_FOREACH(chain_entry, &entry->buckets, entry) iovcnt++;
        STAILQ_INSERT_TAIL(&ctx->pool_head, entry, next);
    }
    struct iovec *buckets = kv_calloc(iovcnt, sizeof(struct iovec)), *p = buckets;
    uint32_t len = 0;
    STAILQ_FOREACH(entry, &ctx->pool_head, next) {
        verify_buckets(entry);
        entry->offset = (self->log.tail + len) % self->log.size;
        len += TAILQ_FIRST(&entry->buckets)->bucket->chain_length;
        TAILQ_FOREACH(chain_entry, &entry->buckets, entry) {
            p->iov_base = chain_entry->bucket;
            p->iov_len = chain_entry->len;
            ++p;
        }
    }
    kv_bucket_log_writev(self, buckets, iovcnt, pool_write_cb, ctx);
    kv_free(buckets);
}

void kv_bucket_pool_put(struct kv_bucket_log *self, struct kv_bucket_pool *entry, bool write_back, kv_circular_log_io_cb cb,
                        void *cb_arg) {
    assert(entry && entry->is_valid);
    if (write_back) {
        struct kv_bucket_pool_head pool_head;
        STAILQ_INIT(&pool_head);
        STAILQ_INSERT_HEAD(&pool_head, entry, next);
        kv_bucket_pool_put_bulk(self, &pool_head, cb, cb_arg);
        return;
    }
    if (--entry->ref_cnt == 0) free_pool_entry(self, entry);
    if (cb) cb(true, cb_arg);
}
