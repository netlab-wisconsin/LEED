#ifndef _KV_CIRCULAR_LOG_H_
#define _KV_CIRCULAR_LOG_H_
#include <sys/uio.h>

#include "kv_storage.h"
struct kv_circular_log {
    struct kv_storage *storage;
    uint64_t base, size;
    uint64_t head, tail;
};
#define KV_CIRCULAR_LOG_ERROR_OFFSET INT32_MAX
typedef kv_storage_io_cb kv_circular_log_io_cb;
static inline uint64_t kv_circular_log_length(struct kv_circular_log *self) {
    return (self->size + self->tail - self->head) % self->size;
}
void kv_circular_log_init(struct kv_circular_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                          uint64_t tail);
void kv_circular_log_read(struct kv_circular_log *self, uint64_t offset, void *blocks, uint64_t n, kv_circular_log_io_cb cb,
                          void *cb_arg);
void kv_circular_log_writev(struct kv_circular_log *self, struct iovec *blocks, int iovcnt, kv_circular_log_io_cb cb,
                            void *cb_arg);
static inline void kv_circular_log_write(struct kv_circular_log *self, void *blocks, uint64_t n, kv_circular_log_io_cb cb,
                                         void *cb_arg) {
    struct iovec iov = {.iov_base = blocks, .iov_len = n};
    kv_circular_log_writev(self, &iov, 1, cb, cb_arg);
}
void kv_circular_log_fini(struct kv_circular_log *self);
#endif