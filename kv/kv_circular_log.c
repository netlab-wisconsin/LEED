#include "kv_circular_log.h"

#include <assert.h>
#include <stdio.h>

#include "kv_app.h"
#include "memery_operation.h"
// --- init & fini ---
void kv_circular_log_init(struct kv_circular_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                          uint64_t tail, uint64_t fetch_buf_size, uint64_t fetch_len) {
    self->storage = storage;
    self->base = base;
    self->size = size;
    self->head = head;
    self->tail = tail;
    self->thread_index = kv_app_get_thread_index();
    kv_memset(&self->fetch, 0, sizeof(self->fetch));
    self->fetch.size = fetch_buf_size + 1;
    self->fetch.fetch_len = fetch_len;
    self->fetch.buffer = kv_storage_blk_alloc(storage, self->fetch.size + 1);
    self->fetch.valid = kv_malloc(self->fetch.size);
    kv_memset(self->fetch.valid, 0, self->fetch.size);
}

void kv_circular_log_fini(struct kv_circular_log *self) {
    kv_storage_free(self->fetch.buffer);
    kv_free(self->fetch.valid);
}

// --- fetch ---
#define FETCH_CON_IO 32

static void copy_to_fetch_buf(struct kv_circular_log *self, struct iovec *blocks, int iovcnt) {
    struct kv_circular_log_fetch *fetch = &self->fetch;
    if (kv_circular_log_length(self) >= fetch->size - 1) return;
    for (int i = 0; i < iovcnt; i++) {
        uint64_t empty_space = kv_circular_log_empty_space(fetch);
        if (empty_space == 0) break;
        uint64_t len = blocks[i].iov_len < empty_space ? blocks[i].iov_len : empty_space;
        uint64_t block_size = self->storage->block_size;
        if (fetch->tail + len > fetch->size) {
            uint64_t len0 = fetch->size - fetch->tail;
            kv_memcpy(fetch->buffer + fetch->tail * block_size, blocks[i].iov_base, len0 * block_size);
            kv_memset(fetch->valid + fetch->tail, 1, len0);
            kv_memcpy(fetch->buffer, (uint8_t *)blocks[i].iov_base + len0 * block_size, (len - len0) * block_size);
            kv_memset(fetch->valid, 1, len - len0);
        } else {
            kv_memcpy(fetch->buffer + fetch->tail * block_size, blocks[i].iov_base, len * block_size);
            kv_memset(fetch->valid + fetch->tail, 1, len);
        }
        fetch->tail = (fetch->tail + len) % fetch->size;
    }
    fetch->tail1 = fetch->tail;
}

struct fetch_ctx {
    struct kv_circular_log *self;
    uint64_t offset;
    uint64_t n;
    int iovcnt;
    struct iovec iov[2];
};
static void _fetch(bool success, void *arg) {
    struct fetch_ctx *ctx = arg;
    struct kv_circular_log *self = ctx->self;
    struct kv_circular_log_fetch *fetch = &self->fetch;
    if (success) {
        for (int i = 0; i < ctx->iovcnt; i++) {
            uint8_t *base = fetch->valid + ((uint8_t *)ctx->iov[i].iov_base - fetch->buffer) / self->storage->block_size;
            for (size_t j = 0; j < ctx->iov[i].iov_len; j++) base[j] = true;
        }
        for (; kv_circular_log_empty_space(fetch) != 0; fetch->tail = (fetch->tail + 1) % fetch->size)
            if (!fetch->valid[fetch->tail]) break;
    } else {
        fprintf(stderr, "circular_log_fetch: IO error, retrying ...\n");
        kv_circular_log_readv(self, ctx->offset, ctx->iov, ctx->iovcnt, _fetch, ctx);
        return;
    }
    uint64_t len = (fetch->size - fetch->head + fetch->tail1) % fetch->size;
    uint64_t fetching_len = (fetch->size - fetch->tail + fetch->tail1) % fetch->size;
    uint64_t remaining_len = fetch->size - 1 - len;
    ctx->offset = (self->head + len) % self->size;
    ctx->n = (self->size - ctx->offset + self->tail) % self->size;
    ctx->n = ctx->n < fetch->fetch_len ? ctx->n : fetch->fetch_len;
    ctx->n = ctx->n < remaining_len ? ctx->n : remaining_len;
    if (ctx->n < fetch->fetch_len || kv_circular_log_length(fetch) == kv_circular_log_length(self) ||
        fetching_len >= fetch->fetch_len * FETCH_CON_IO) {
        kv_free(ctx);
    } else {
        if (ctx->n + fetch->tail1 > fetch->size) {
            ctx->iov[0].iov_base = fetch->buffer + fetch->tail1 * self->storage->block_size;
            ctx->iov[0].iov_len = fetch->size - fetch->tail1;
            ctx->iov[1].iov_base = fetch->buffer;
            ctx->iov[1].iov_len = ctx->n - ctx->iov[0].iov_len;
            ctx->iovcnt = 2;
        } else {
            ctx->iov[0].iov_base = fetch->buffer + fetch->tail1 * self->storage->block_size;
            ctx->iov[0].iov_len = ctx->n;
            ctx->iovcnt = 1;
        }
        kv_circular_log_readv(self, ctx->offset, ctx->iov, ctx->iovcnt, _fetch, ctx);
        fetch->tail1 = (fetch->tail1 + ctx->n) % fetch->size;
    }
}

void kv_circular_log_move_head(struct kv_circular_log *self, uint64_t n) {
    struct kv_circular_log_fetch *fetch = &self->fetch;
    assert(kv_circular_log_length(fetch) >= n);
    self->head = (self->head + n) % self->size;
    for (size_t i = 0; i < n; i++) fetch->valid[(fetch->head + i) % fetch->size] = 0;
    fetch->head = (fetch->head + n) % fetch->size;

    while (true) {
        struct fetch_ctx *ctx = kv_malloc(sizeof(struct fetch_ctx));
        kv_memset(ctx, 0, sizeof(struct fetch_ctx));
        ctx->self = self;
        uint64_t tail1 = fetch->tail1;
        _fetch(true, ctx);
        if (tail1 == fetch->tail1) break;
    }
}

// offset start from head
void kv_circular_log_fetch(struct kv_circular_log *self, uint64_t offset, uint64_t n, struct iovec iov[2]) {
    offset = (self->size - self->head + offset) % self->size;
    struct kv_circular_log_fetch *fetch = &self->fetch;
    assert(offset < kv_circular_log_length(fetch));
    offset = (fetch->head + offset) % fetch->size;
    assert((fetch->size + fetch->tail - offset) % fetch->size >= n);
    iov[0].iov_base = fetch->buffer + offset * self->storage->block_size;
    iov[0].iov_len = offset + n > fetch->size ? fetch->size - offset : n;
    iov[1].iov_base = fetch->buffer;
    iov[1].iov_len = n - iov[0].iov_len;
}

void kv_circular_log_fetch_one(struct kv_circular_log *self, uint64_t offset, void **buf) {
    offset = (self->size - self->head + offset) % self->size;
    assert(offset < kv_circular_log_length(&self->fetch));
    offset = (self->fetch.head + offset) % self->fetch.size;
    *buf = self->fetch.buffer + offset * self->storage->block_size;
}

// --- iov ---
struct iov_ctx {
    struct iovec *blocks;
    kv_circular_log_io_cb cb;
    void *cb_arg;
    uint8_t io_cnt;
};

static void iov_cb(bool success, void *arg) {
    struct iov_ctx *ctx = arg;
    if (--ctx->io_cnt) return;
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx->blocks);
    kv_free(ctx);
}
static void iov_fail_cb(void *arg) { iov_cb(false, arg); }

void kv_circular_log_iov(struct kv_circular_log *self, uint64_t offset, struct iovec *blocks, int iovcnt,
                         kv_circular_log_io_cb cb, void *cb_arg, enum kv_circular_log_operation operation) {
    struct iov_ctx *ctx = kv_malloc(sizeof(struct iov_ctx));
    ctx->cb = cb;
    ctx->cb_arg = cb_arg;
    ctx->blocks = kv_calloc(iovcnt + 1, sizeof(struct iovec));
    uint64_t n = 0, remaining_blocks = self->size - offset;
    struct iovec *blocks2 = NULL;
    int i2;
    bool is_splitted = false;
    for (int i = 0; i < iovcnt; i++) {
        assert(((uint64_t)blocks[i].iov_base & 0x3) == 0);
        n += blocks[i].iov_len;
        if (!is_splitted && n >= remaining_blocks) {
            is_splitted = true;
            i2 = i + 1;
            ctx->blocks[i2].iov_len = (n - remaining_blocks) * self->storage->block_size;
            ctx->blocks[i].iov_len = blocks[i].iov_len * self->storage->block_size - ctx->blocks[i2].iov_len;
            ctx->blocks[i].iov_base = blocks[i].iov_base;
            ctx->blocks[i2].iov_base = (uint8_t *)blocks[i].iov_base + ctx->blocks[i].iov_len;
            blocks2 = ctx->blocks[i2].iov_len ? ctx->blocks + i2 : (i2 == iovcnt ? NULL : ctx->blocks + i2 + 1);
        } else {
            ctx->blocks[i + (int)is_splitted].iov_base = blocks[i].iov_base;
            ctx->blocks[i + (int)is_splitted].iov_len = blocks[i].iov_len * self->storage->block_size;
        }
    }
    switch (operation) {
        case CIRCULAR_LOG_READ:
            assert((self->size + offset - self->head) % self->size < kv_circular_log_length(self));
            assert((self->size + self->tail - offset) % self->size >= n);
            if (blocks2) {
                ctx->io_cnt = 2;
                kv_storage_read_blocks(self->storage, ctx->blocks, i2, self->base + offset, remaining_blocks, iov_cb, ctx);
                kv_storage_read_blocks(self->storage, blocks2, ctx->blocks + iovcnt + 1 - blocks2, self->base,
                                       n - remaining_blocks, iov_cb, ctx);
            } else {
                ctx->io_cnt = 1;
                kv_storage_read_blocks(self->storage, ctx->blocks, iovcnt, self->base + offset, n, iov_cb, ctx);
            }
            break;
        case CIRCULAR_LOG_APPEND:
            offset = self->tail;
            if (kv_circular_log_empty_space(self) < n) {
                fprintf(stderr, "kv_circular_log_append: No more space!\n");
                ctx->io_cnt = 1;
                kv_app_send(self->thread_index, iov_fail_cb, ctx);
                return;
            }
            copy_to_fetch_buf(self, blocks, iovcnt);
            self->tail = (self->tail + n) % self->size;
            // fall through
        case CIRCULAR_LOG_WRITE:
            if (blocks2) {
                ctx->io_cnt = 2;
                kv_storage_write_blocks(self->storage, ctx->blocks, i2, self->base + offset, remaining_blocks, iov_cb, ctx);
                kv_storage_write_blocks(self->storage, blocks2, ctx->blocks + iovcnt + 1 - blocks2, self->base,
                                        n - remaining_blocks, iov_cb, ctx);
            } else {
                ctx->io_cnt = 1;
                kv_storage_write_blocks(self->storage, ctx->blocks, iovcnt, self->base + offset, n, iov_cb, ctx);
            }
    }
}
