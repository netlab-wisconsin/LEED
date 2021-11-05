#include "kv_circular_log.h"

#include <assert.h>
#include <stdio.h>

#include "kv_app.h"
#include "memery_operation.h"
void kv_circular_log_init(struct kv_circular_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                          uint64_t tail) {
    self->storage = storage;
    self->base = base;
    self->size = size;
    self->head = head;
    self->tail = tail;
}

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
            self->tail = (self->tail + n) % self->size;
            if (kv_circular_log_empty_space(self) < n) {
                fprintf(stderr, "kv_circular_log_append: No more space!\n");
                ctx->io_cnt = 1;
                kv_app_send_msg(kv_app_get_thread(), iov_fail_cb, ctx);
                return;
            }
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
void kv_circular_log_fini(struct kv_circular_log *self) {}