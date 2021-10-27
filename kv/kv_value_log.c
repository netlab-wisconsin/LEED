#include "kv_value_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/uio.h>

#include "memery_operation.h"
static inline uint64_t align(struct kv_value_log *self, uint64_t size) {
    if (size & self->blk_mask) return (size & ~self->blk_mask) + self->storage->block_size;
    return size;
}
uint64_t kv_value_log_write(struct kv_value_log *self, uint8_t *value, uint32_t value_length, kv_value_log_io_cb cb,
                            void *cb_arg) {
    if ((self->size + self->head - 1 - self->tail) % self->size < 2 * self->storage->block_size + value_length) {
        fprintf(stderr, "kv_value_log_write: Not enough space!\n");
        cb(false, cb_arg);
        return KV_VALUE_LOG_ERROR_OFFSET;
    }
    uint64_t offset = align(self, self->tail) % self->size;
    if (offset + value_length > self->size) {
        // TODO: 2 writes
        fprintf(stderr, "kv_value_log_write: Not implemented.\n");
        cb(false, cb_arg);
        return KV_VALUE_LOG_ERROR_OFFSET;
    } else
        kv_storage_write(self->storage, value, 0, self->base + offset, align(self, value_length), cb, cb_arg);
    self->tail = (offset + value_length) % self->size;
    return offset;
}
struct read_ctx {
    uint8_t *value;
    kv_value_log_io_cb cb;
    void *cb_arg;
    uint8_t *buf;
    struct iovec iov[2];
    uint16_t buf_offset, len_in_buf;
};
static inline void read_ctx_init(struct read_ctx *ctx, uint8_t *value, kv_value_log_io_cb cb, void *cb_arg) {
    ctx->value = value;
    ctx->cb = cb;
    ctx->cb_arg = cb_arg;
}

static void read_cb(bool success, void *cb_arg) {
    struct read_ctx *ctx = (struct read_ctx *)cb_arg;
    kv_memcpy(ctx->value, ctx->buf + ctx->buf_offset, ctx->len_in_buf);
    kv_storage_free(ctx->buf);
    if (ctx->cb) ctx->cb(success, cb_arg);
    kv_free(ctx);
}

void kv_value_log_read(struct kv_value_log *self, uint64_t offset, uint8_t *value, uint32_t value_length, kv_value_log_io_cb cb,
                       void *cb_arg) {
    if (offset + value_length > self->size) {
        // TODO: 2 reads
        fprintf(stderr, "kv_value_log_read: Not implemented.\n");
        cb(false, cb_arg);
        return;
    }
    if (offset & self->blk_mask) {
        struct read_ctx *ctx = kv_malloc(sizeof(struct read_ctx));
        read_ctx_init(ctx, value, cb, cb_arg);
        ctx->buf_offset = offset & self->blk_mask;
        ctx->len_in_buf = self->storage->block_size - ctx->buf_offset;
        ctx->buf = kv_storage_blk_alloc(self->storage, 1);
        ctx->iov[0] = (struct iovec){ctx->buf, self->storage->block_size};  // self->base + (offset | ~self->blk_mask)
        ctx->iov[1] = (struct iovec){value + ctx->len_in_buf, align(self, value_length - ctx->len_in_buf)};
        kv_storage_read(self->storage, ctx->iov, 2, self->base + (offset & ~self->blk_mask),
                        ctx->iov[0].iov_len + ctx->iov[1].iov_len, read_cb, ctx);
    } else
        kv_storage_read(self->storage, value, 0, self->base + offset, align(self, value_length), cb, cb_arg);
}

void kv_value_log_init(struct kv_value_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                       uint64_t tail) {
    self->blk_mask = storage->block_size - 1;
    assert(!(base & self->blk_mask || size & self->blk_mask));
    self->storage = storage;
    self->base = base;
    self->size = size;
    self->head = head;
    self->tail = tail;
}
void kv_value_log_fini(struct kv_value_log *self) {}