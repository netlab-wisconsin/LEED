#include "kv_circular_log.h"

#include <assert.h>
#include <stdio.h>

#include "memery_operation.h"
void kv_circular_log_init(struct kv_circular_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                          uint64_t tail) {
    self->storage = storage;
    self->base = base;
    self->size = size;
    self->head = head;
    self->tail = tail;
}
struct read_ctx {
    struct kv_circular_log *self;
    uint64_t offset;
    void *blocks;
    uint64_t n;
    kv_circular_log_io_cb cb;
    void *cb_arg;
};
#define read_ctx_init(ctx, self, _blocks, _n, cb, cb_arg) \
    do {                                                  \
        ctx->self = self;                                 \
        ctx->blocks = _blocks;                            \
        ctx->n = _n;                                      \
        ctx->cb = cb;                                     \
        ctx->cb_arg = cb_arg;                             \
    } while (0)
static void read_cb(bool success, void *arg) {
    struct read_ctx *ctx = arg;
    if (success)
        kv_storage_read_blocks(ctx->self->storage, ctx->blocks, 0, ctx->self->base, ctx->n, ctx->cb, ctx->cb_arg);
    else if (ctx->cb)
        ctx->cb(false, ctx->cb_arg);
    kv_free(ctx);
}
void kv_circular_log_read(struct kv_circular_log *self, uint64_t offset, void *blocks, uint64_t n, kv_circular_log_io_cb cb,
                          void *cb_arg) {
    assert((self->size + offset - self->head) % self->size < kv_circular_log_length(self));
    uint64_t remaining_blocks = self->size - offset;
    if (remaining_blocks >= n) {
        kv_storage_read_blocks(self->storage, blocks, 0, self->base + offset, n, cb, cb_arg);
    } else {
        uint64_t remaining_length = remaining_blocks * self->storage->block_size;
        struct read_ctx *ctx = kv_malloc(sizeof(struct read_ctx));
        read_ctx_init(ctx, self, (uint8_t *)blocks + remaining_length, n - remaining_blocks, cb, cb_arg);
        kv_storage_read_blocks(self->storage, blocks, 0, self->base + offset, remaining_blocks, read_cb, ctx);
    }
}

struct writev_ctx {
    struct kv_circular_log *self;
    struct iovec *blocks;
    int iovcnt;
    kv_circular_log_io_cb cb;
    void *cb_arg;
    uint64_t length;
};
#define writev_ctx_init(ctx, self, _blocks, _iovcnt, cb, cb_arg) \
    do {                                                         \
        ctx->self = self;                                        \
        ctx->blocks = _blocks;                                   \
        ctx->iovcnt = _iovcnt;                                   \
        ctx->cb = cb;                                            \
        ctx->cb_arg = cb_arg;                                    \
    } while (0)

static void writev_cb(bool success, void *arg) {
    struct writev_ctx *ctx = arg;
    if (success)
        kv_storage_write_blocks(ctx->self->storage, ctx->blocks, ctx->iovcnt, ctx->self->base, ctx->length, ctx->cb,
                                ctx->cb_arg);
    else if (ctx->cb)
        ctx->cb(false, ctx->cb_arg);
    kv_free(ctx->blocks);
    kv_free(ctx);
}

void kv_circular_log_writev(struct kv_circular_log *self, struct iovec *blocks, int iovcnt, kv_circular_log_io_cb cb,
                            void *cb_arg) {
    uint64_t n = 0;
    for (int i = 0; i < iovcnt; i++) {
        n += blocks[i].iov_len;
        blocks[i].iov_len *= self->storage->block_size;
    }
    if (self->size - 1 - kv_circular_log_length(self) < n) {
        fprintf(stderr, "kv_circular_log_writes: No more space!\n");
        cb(false, cb_arg);
        return;
    }
    uint64_t remaining_blocks = self->size - self->tail;
    if (remaining_blocks >= n) {
        kv_storage_write_blocks(self->storage, blocks, iovcnt, self->base + self->tail, n, cb, cb_arg);
    } else {  // unlikely
        uint64_t remaining_length = remaining_blocks * self->storage->block_size;
        uint64_t length = 0, i;
        for (i = 0; length + blocks[i].iov_len <= remaining_length; i++) length += blocks[i].iov_len;
        struct iovec *_blocks = kv_calloc(iovcnt - i, sizeof(struct iovec));
        kv_memcpy(_blocks + 1, blocks + i + 1, (iovcnt - i - 1) * sizeof(struct iovec));
        struct writev_ctx *ctx = kv_malloc(sizeof(struct writev_ctx));
        writev_ctx_init(ctx, self, _blocks, iovcnt - i, cb, cb_arg);
        _blocks->iov_len = blocks[i].iov_len + length - remaining_length;
        ctx->length = n - remaining_blocks;
        blocks[i].iov_len = remaining_length - length;
        _blocks->iov_base = (uint8_t *)blocks[i].iov_base + blocks[i].iov_len;
        kv_storage_write_blocks(self->storage, blocks, iovcnt, self->base + self->tail, remaining_blocks, writev_cb, ctx);
    }
    self->tail = (self->tail + n) % self->size;
}
void kv_circular_log_fini(struct kv_circular_log *self) {}