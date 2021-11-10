#ifndef _KV_CIRCULAR_LOG_H_
#define _KV_CIRCULAR_LOG_H_
#include <sys/uio.h>

#include "kv_storage.h"
struct kv_circular_log_fetch {
    uint32_t io_cnt;
    uint64_t fetch_len;
    uint64_t head, tail, tail1, size;
    uint8_t *buffer;
    uint8_t *valid;
};

struct kv_circular_log {
    struct kv_storage *storage;
    uint64_t base, size;
    uint64_t head, tail;
    struct kv_circular_log_fetch fetch;
};
typedef kv_storage_io_cb kv_circular_log_io_cb;
enum kv_circular_log_operation { CIRCULAR_LOG_READ, CIRCULAR_LOG_APPEND, CIRCULAR_LOG_WRITE };

#define kv_circular_log_length(self) (((self)->size + (self)->tail - (self)->head) % (self)->size)
#define kv_circular_log_empty_space(self) ((self)->size - 1 - kv_circular_log_length(self))

void kv_circular_log_init(struct kv_circular_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                          uint64_t tail, uint64_t fetch_buf_size, uint64_t fetch_len);
void kv_circular_log_fini(struct kv_circular_log *self);

void kv_circular_log_iov(struct kv_circular_log *self, uint64_t offset, struct iovec *blocks, int iovcnt,
                         kv_circular_log_io_cb cb, void *cb_arg, enum kv_circular_log_operation operation);
static inline void kv_circular_log_read(struct kv_circular_log *self, uint64_t offset, void *blocks, uint64_t n,
                                        kv_circular_log_io_cb cb, void *cb_arg) {
    struct iovec iov = {.iov_base = blocks, .iov_len = n};
    kv_circular_log_iov(self, offset, &iov, 1, cb, cb_arg, CIRCULAR_LOG_READ);
}
static inline void kv_circular_log_readv(struct kv_circular_log *self, uint64_t offset, struct iovec *blocks, int iovcnt,
                                         kv_circular_log_io_cb cb, void *cb_arg) {
    kv_circular_log_iov(self, offset, blocks, iovcnt, cb, cb_arg, CIRCULAR_LOG_READ);
}

static inline void kv_circular_log_write(struct kv_circular_log *self, uint64_t offset, void *blocks, uint64_t n,
                                         kv_circular_log_io_cb cb, void *cb_arg) {
    struct iovec iov = {.iov_base = blocks, .iov_len = n};
    kv_circular_log_iov(self, offset, &iov, 1, cb, cb_arg, CIRCULAR_LOG_WRITE);
}
static inline void kv_circular_log_writev(struct kv_circular_log *self, uint64_t offset, struct iovec *blocks, int iovcnt,
                                          kv_circular_log_io_cb cb, void *cb_arg) {
    kv_circular_log_iov(self, offset, blocks, iovcnt, cb, cb_arg, CIRCULAR_LOG_WRITE);
}

static inline void kv_circular_log_append(struct kv_circular_log *self, void *blocks, uint64_t n, kv_circular_log_io_cb cb,
                                          void *cb_arg) {
    struct iovec iov = {.iov_base = blocks, .iov_len = n};
    kv_circular_log_iov(self, self->tail, &iov, 1, cb, cb_arg, CIRCULAR_LOG_APPEND);
}
static inline void kv_circular_log_appendv(struct kv_circular_log *self, struct iovec *blocks, int iovcnt,
                                           kv_circular_log_io_cb cb, void *cb_arg) {
    kv_circular_log_iov(self, self->tail, blocks, iovcnt, cb, cb_arg, CIRCULAR_LOG_APPEND);
}

// offset start from head
void kv_circular_log_fetch(struct kv_circular_log *self, uint64_t offset, uint64_t n, struct iovec iov[2]);
void kv_circular_log_fetch_one(struct kv_circular_log *self, uint64_t offset, void **buf);
void kv_circular_log_move_head(struct kv_circular_log *self, uint64_t n);
#endif