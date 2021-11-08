#include "kv_bucket_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/uio.h>

#include "kv_app.h"
#include "kv_circular_log.h"
#include "memery_operation.h"

#define INIT_CON_IO_NUM 8
#define COMPACT_BUF(self) ((self)->compact.buffer[(self)->compact.i])
#define COMPACT_PREFETCH_BUF(self) ((self)->compact.buffer[!(self)->compact.i])

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
    for (size_t i = 0; i < 2; i++) self->compact.buffer[i] = kv_storage_zblk_alloc(self->log.storage, self->compact.buf_len);
    for (size_t i = 0; i < self->compact.buf_len; i++) {
        COMPACT_BUF(self)[i].index = i;
        COMPACT_BUF(self)[i].chain_length = 1;
    }
    self->compact.iov = kv_calloc(self->compact.buf_len, sizeof(struct iovec));

    self->bucket_meta = kv_calloc(self->bucket_num, sizeof(struct kv_bucket_meta));
    kv_memset(self->bucket_meta, 0, sizeof(struct kv_bucket_meta) * self->bucket_num);
    for (size_t i = 0; i < self->bucket_num; i++) {
        self->bucket_meta[i].bucket_offset = self->log.head + i;
        self->bucket_meta[i].chain_length = 1;
    }
    self->compact.state.running = false;
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
    uint32_t n = self->log.size - self->tail;
    if (n == 0) {
        if (--ctx->init_ctx->io_num == 0) {
            init(self, ctx->init_ctx->cb, ctx->init_ctx->cb_arg);
            kv_free(ctx->init_ctx);
        }
        kv_storage_free(ctx);
        return;
    }
    if (n <= self->compact.buf_len)
        kv_bucket_log_move_head(self, self->log.size - self->bucket_num);
    else
        n = self->compact.buf_len;
    for (size_t i = 0; i < n; i++) {
        if (self->tail + i < self->log.size - self->bucket_num)
            ctx->buckets[i].index = UINT32_MAX;
        else
            ctx->buckets[i].index = self->tail + i + self->bucket_num - self->log.size;
        ctx->buckets[i].chain_length = 1;
    }
    kv_bucket_log_write(self, ctx->buckets, n, init_write_cb, ctx);
}

void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint64_t base, uint32_t size,
                        uint32_t log_num_buckets, uint32_t compact_buf_len, kv_circular_log_io_cb cb, void *cb_arg) {
    assert(storage->block_size == sizeof(struct kv_bucket));
    kv_memset(self, 0, sizeof(struct kv_bucket_log));
    kv_circular_log_init(&self->log, storage, base, size, 0, 0);
    self->size = size << 1;
    self->compact.buf_len = compact_buf_len;
    self->bucket_num = 1U << log_num_buckets;
    struct init_ctx *ctx = kv_malloc(sizeof(struct init_ctx));
    ctx->self = self;
    ctx->cb = cb;
    ctx->cb_arg = cb_arg;
    ctx->io_num = INIT_CON_IO_NUM;
    ctx->success = true;
    self->compact.state.running = true;
    for (size_t i = 0; i < ctx->io_num; i++) {
        struct init_write_ctx *buf =
            kv_storage_zmalloc(storage, sizeof(struct init_write_ctx) + compact_buf_len * sizeof(struct kv_bucket));
        buf->init_ctx = ctx;
        init_write_cb(true, buf);
    }
}
static void fini(void *arg) { kv_bucket_log_fini(arg); }

void kv_bucket_log_fini(struct kv_bucket_log *self) {
    if (self->compact.state.running)
        kv_app_send_msg(kv_app_get_thread(), fini, self);
    else {
        kv_free(self->bucket_meta);
        kv_free(self->compact.iov);
        kv_storage_free(self->compact.buffer[0]);
        kv_storage_free(self->compact.buffer[1]);
        kv_circular_log_fini(&self->log);
    }
}

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
static bool bucket_lock_nowait(struct kv_bucket_log *self, uint32_t index) {
    assert(index < self->bucket_num);
    if (self->bucket_meta[index].lock) return false;
    self->bucket_meta[index].lock = 1;
    return true;
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

static void compact_cb(bool success, void *arg) {
    struct kv_bucket_log *self = arg;
    self->compact.state.err = self->compact.state.err || !success;
    if (--self->compact.state.io_cnt == 0) {
        self->compact.state.running = false;
        if (self->compact.state.err) {
            fprintf(stderr, "compact_cb: IO error!\n");
            self->compact.state.err = 0;
            return;
        }
        uint32_t n = 0;
        for (size_t i = 0; i < self->compact.iovcnt; i++) {
            struct kv_bucket *bucket = self->compact.iov[i].iov_base;
            self->bucket_meta[bucket->index].bucket_offset = (self->compact.offset + n) % self->log.size;
            kv_bucket_unlock(self, bucket->index);
            // verify_buckets(bucket, bucket->index, self->bucket_meta[bucket->index].chain_length);
            n += self->compact.iov[i].iov_len;
        }
        kv_bucket_log_move_head(self, self->compact.length);
        self->compact.i = !self->compact.i;
        // printf("compression ratio: %lf\n", (double)n / self->compact.length);
    }
}

static void compact(struct kv_bucket_log *self) {
    if (kv_circular_log_empty_space(&self->log) >= self->compact.buf_len << 4 || self->compact.state.running) return;
    self->compact.state.running = true;
    self->compact.iovcnt = 0;
    self->compact.length = self->compact.buf_len;
    for (size_t i = 0; i < self->compact.buf_len; i += COMPACT_BUF(self)[i].chain_length) {
        assert(COMPACT_BUF(self)[i].chain_index == 0);
        uint32_t bucket_offset = (self->log.head + i) % self->log.size;
        struct kv_bucket_meta *meta = self->bucket_meta + COMPACT_BUF(self)[i].index;
        if (COMPACT_BUF(self)[i].chain_length + i > self->compact.buf_len) {
            if (meta->bucket_offset == bucket_offset && !meta->lock)
                self->compact.length = i;
            else
                self->compact.length = COMPACT_BUF(self)[i].chain_length + i;
            break;
        }
        if (meta->bucket_offset == bucket_offset && bucket_lock_nowait(self, COMPACT_BUF(self)[i].index))
            self->compact.iov[self->compact.iovcnt++] =
                (struct iovec){COMPACT_BUF(self) + i, COMPACT_BUF(self)[i].chain_length};
    }
    self->compact.state.io_cnt = 2;
    self->compact.offset = kv_bucket_log_offset(self);
    if (self->compact.iovcnt)
        kv_bucket_log_writev(self, self->compact.iov, self->compact.iovcnt, compact_cb, self);
    else
        compact_cb(true, self);
    uint32_t offset = (self->log.head + self->compact.length) % self->log.size;
    // prefetch
    // TODO: using bit map
    kv_circular_log_read(&self->log, offset, COMPACT_PREFETCH_BUF(self), self->compact.buf_len, compact_cb, self);
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
