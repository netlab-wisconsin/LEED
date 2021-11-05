#include "kv_bucket_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/uio.h>

#include "kv_circular_log.h"
#include "memery_operation.h"
void kv_bucket_log_init(struct kv_bucket_log *self, struct kv_storage *storage, uint64_t base, uint32_t size, uint32_t head,
                        uint32_t tail) {
    assert(storage->block_size == sizeof(struct kv_bucket));
    kv_circular_log_init(&self->log, storage, base, size, head % size, tail % size);
    self->size = size << 1;
    self->head = head;
    self->tail = tail;
}

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
}

void kv_bucket_log_fini(struct kv_bucket_log *self) { kv_circular_log_fini(&self->log); }