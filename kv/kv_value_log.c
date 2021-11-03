#include "kv_value_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/uio.h>

#include "memery_operation.h"
static inline uint64_t align(struct kv_value_log *self, uint64_t size) {
    if (size & self->blk_mask) return (size & ~self->blk_mask) + self->log.storage->block_size;
    return size;
}
static inline uint64_t align_blk(struct kv_value_log *self, uint64_t size) { return align(self, size) >> self->blk_shift; }

void kv_value_log_write(struct kv_value_log *self, uint8_t *value, uint32_t value_length, kv_circular_log_io_cb cb,
                        void *cb_arg) {
    kv_circular_log_write(&self->log, value, align_blk(self, value_length), cb, cb_arg);
}

struct read_ctx {
    uint8_t *value;
    kv_circular_log_io_cb cb;
    void *cb_arg;
    uint8_t *buf;
    uint16_t buf_offset, len_in_buf;
};

static void read_cb(bool success, void *cb_arg) {
    struct read_ctx *ctx = (struct read_ctx *)cb_arg;
    kv_memcpy(ctx->value, ctx->buf + ctx->buf_offset, ctx->len_in_buf);
    kv_storage_free(ctx->buf);
    if (ctx->cb) ctx->cb(success, cb_arg);
    kv_free(ctx);
}

void kv_value_log_read(struct kv_value_log *self, uint64_t offset, uint8_t *value, uint32_t value_length,
                       kv_circular_log_io_cb cb, void *cb_arg) {
    assert((offset & 0x3) == 0);
    if (offset & self->blk_mask) {
        struct read_ctx *ctx = kv_malloc(sizeof(struct read_ctx));
        ctx->value = value;
        ctx->cb = cb;
        ctx->cb_arg = cb_arg;
        ctx->buf_offset = offset & self->blk_mask;
        ctx->len_in_buf = self->log.storage->block_size - ctx->buf_offset;
        ctx->buf = kv_storage_blk_alloc(self->log.storage, 1);
        struct iovec iov[2] = {{ctx->buf, 1}, {value + ctx->len_in_buf, align_blk(self, value_length - ctx->len_in_buf)}};
        kv_circular_log_readv(&self->log, offset >> self->blk_shift, iov, 2, read_cb, ctx);
    } else
        kv_circular_log_read(&self->log, offset >> self->blk_shift, value, align_blk(self, value_length), cb, cb_arg);
}

void kv_value_log_init(struct kv_value_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                       uint64_t tail) {
    for (self->blk_shift = 0; !((storage->block_size >> self->blk_shift) & 1); ++self->blk_shift)
        ;
    assert(storage->block_size == 1U << self->blk_shift);
    self->blk_mask = storage->block_size - 1;
    assert(!(base & self->blk_mask || size & self->blk_mask));
    kv_circular_log_init(&self->log, storage, base >> self->blk_shift, align(self, size), head >> self->blk_shift,
                         align(self, tail));
    self->head = head;
}

void kv_value_log_fini(struct kv_value_log *self) { kv_circular_log_fini(&self->log); }