#include "kv_bucket_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>
#include <sys/uio.h>

#include "kv_app.h"
#include "kv_circular_log.h"
#include "memery_operation.h"

// --- lock & unlock ---
void kv_bucket_lock(struct kv_bucket_log *self, uint32_t index, kv_task_cb cb, void *cb_arg) {
    assert(index < self->bucket_num);
    assert(cb);
    if (self->bucket_meta[index].lock) {
        kv_waiting_map_put(&self->waiting_map, index, cb, cb_arg);
    } else {
        self->bucket_meta[index].lock = 1;
        cb(cb_arg);
    }
}

void kv_bucket_unlock(struct kv_bucket_log *self, uint32_t index) {
    assert(index < self->bucket_num);
    assert(self->bucket_meta[index].lock);
    struct kv_waiting_task task = kv_waiting_map_get(&self->waiting_map, index);
    if (task.cb)
        kv_app_send_msg(kv_app_get_thread(), task.cb, task.cb_arg);
    else
        self->bucket_meta[index].lock = 0;
}

// --- compact ---
struct bucket_index_entry {
    uint32_t index;
    uint32_t bucket_offset;
    SLIST_ENTRY(bucket_index_entry) next;
};

struct kv_bucket_log_compact {
    uint32_t length, buf_len;
    struct {
        uint32_t err : 1;
        uint32_t running : 1;
        uint32_t io_cnt : 30;
    } state;
    SLIST_HEAD(, bucket_index_entry) locked_buckets;
};

static void compact_cb(bool success, void *arg) {
    struct kv_bucket_log *self = arg;
    struct kv_bucket_log_compact *compact = self->compact;
    compact->state.err = compact->state.err || !success;
    if (--compact->state.io_cnt == 0) {
        compact->state.running = false;
        if (compact->state.err) {
            fprintf(stderr, "compact_cb: IO error!\n");
            compact->state.err = 0;
            return;
        }
        while (!SLIST_EMPTY(&compact->locked_buckets)) {
            struct bucket_index_entry *entry = SLIST_FIRST(&compact->locked_buckets);
            if (entry->bucket_offset != UINT32_MAX) self->bucket_meta[entry->index].bucket_offset = entry->bucket_offset;
            kv_bucket_unlock(self, entry->index);
            SLIST_REMOVE_HEAD(&compact->locked_buckets, next);
            kv_free(entry);
        }
        self->head = (self->head + compact->length) % self->size;
        kv_circular_log_move_head(&self->log, compact->length);
        // printf("compression ratio: %lf\n", (double)n / self->compact.length);
    }
}
static void compact_lock_cb(void *arg) { compact_cb(true, arg); }
static inline void append_iov(struct iovec *iov0, uint32_t *cnt, struct iovec iov[2]) {
    if ((*cnt) == 0 || (struct kv_bucket *)iov0[*cnt - 1].iov_base + iov0[*cnt - 1].iov_len != iov[0].iov_base)
        iov0[(*cnt)++] = iov[0];
    else
        iov0[*cnt - 1].iov_len += iov[0].iov_len;
    if (iov[1].iov_len) iov0[(*cnt)++] = iov[1];
}
static void compact(struct kv_bucket_log *self) {
    struct kv_bucket_log_compact *compact = self->compact;
    if (kv_circular_log_empty_space(&self->log) >= compact->buf_len << 4 || compact->state.running) return;
    compact->state.running = true;
    uint32_t iovcnt = 0;
    struct iovec *iov0 = kv_calloc(compact->buf_len, sizeof(struct iovec));
    struct iovec iov[2];
    compact->state.io_cnt = 1;

    uint32_t offset = kv_bucket_log_offset(self);
    struct kv_bucket *bucket;
    for (compact->length = 0; compact->length < compact->buf_len; compact->length += bucket->chain_length) {
        kv_circular_log_fetch_one(&self->log, compact->length, (void **)&bucket);
        assert(bucket->chain_index == 0);
        uint32_t bucket_offset = (self->log.head + compact->length) % self->log.size;
        struct kv_bucket_meta *meta = self->bucket_meta + bucket->index;
        if (meta->bucket_offset == bucket_offset) {
            struct bucket_index_entry *entry = kv_malloc(sizeof(struct bucket_index_entry));
            entry->index = bucket->index;
            entry->bucket_offset = UINT32_MAX;
            if (!meta->lock) {
                entry->bucket_offset = offset;
                kv_circular_log_fetch(&self->log, compact->length, bucket->chain_length, iov);
                append_iov(iov0, &iovcnt, iov);
                offset = (offset + bucket->chain_length) % self->log.size;
            }
            // if bucket is locked -> wait for lock before commit
            // else -> locked & move the bucket
            compact->state.io_cnt++;
            kv_bucket_lock(self, bucket->index, compact_lock_cb, self);
            SLIST_INSERT_HEAD(&compact->locked_buckets, entry, next);
        }
    }

    if (iovcnt)
        kv_bucket_log_writev(self, iov0, iovcnt, compact_cb, self);
    else
        compact_cb(true, self);
    kv_free(iov0);
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
    ((struct kv_bucket_log_compact *)self->compact)->state.running = false;
    if (cb) cb(true, cb_arg);
}

static void init_write_cb(bool success, void *arg) {
    struct init_write_ctx *ctx = arg;
    struct kv_bucket_log *self = ctx->init_ctx->self;
    struct kv_bucket_log_compact *compact = self->compact;
    if (!(ctx->init_ctx->success = ctx->init_ctx->success && success)) {
        if (--ctx->init_ctx->io_num == 0) {
            if (ctx->init_ctx->cb) ctx->init_ctx->cb(false, ctx->init_ctx->cb_arg);
            kv_free(compact);
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
    if (n <= compact->buf_len) {
        self->head = head;
        self->log.head = head;
    } else
        n = compact->buf_len;
    for (size_t i = 0; i < n; i++) {
        if (self->tail + i < head)
            ctx->buckets[i].index = UINT32_MAX;
        else
            ctx->buckets[i].index = self->tail + i - head;
        ctx->buckets[i].chain_length = 1;
    }
    kv_bucket_log_write(self, ctx->buckets, n, init_write_cb, ctx);
}

void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint64_t base, uint32_t size,
                        uint32_t log_num_buckets, uint32_t compact_buf_len, kv_circular_log_io_cb cb, void *cb_arg) {
    assert(storage->block_size == sizeof(struct kv_bucket));
    kv_memset(self, 0, sizeof(struct kv_bucket_log));
    kv_circular_log_init(&self->log, storage, base, size, 0, 0, compact_buf_len * 4, 32);
    self->size = size << 1;
    self->bucket_num = 1U << log_num_buckets;

    struct kv_bucket_log_compact *compact = kv_malloc(sizeof(struct kv_bucket_log_compact));
    memset(compact, 0, sizeof(struct kv_bucket_log_compact));
    self->compact = compact;
    compact->buf_len = compact_buf_len;
    compact->state.running = true;  // disable compact while init

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
static void fini(void *arg) { kv_bucket_log_fini(arg); }

void kv_bucket_log_fini(struct kv_bucket_log *self) {
    if (((struct kv_bucket_log_compact *)self->compact)->state.running)
        kv_app_send_msg(kv_app_get_thread(), fini, self);
    else {
        kv_free(self->bucket_meta);
        kv_free(self->compact);
        kv_circular_log_fini(&self->log);
    }
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
    compact(self);
}
