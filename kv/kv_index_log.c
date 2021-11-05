#include "kv_index_log.h"

#include <assert.h>
#include <stdio.h>

#include "kv_app.h"
#include "memery_operation.h"
#define OFFSET_TO_BLOCK(offset) ((offset) >> (7 + 8))
#define OFFSET_TO_INDEX(offset) (((offset) >> 8) & 0x7F)

#define BUF(buf, self) ((self)->buf##_buf[(self)->buf##_buf_i])
#define PREFETCH_BUF(buf, self) ((self)->buf##_buf[!(self)->buf##_buf_i])

void kv_index_log_init(struct kv_index_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                       uint64_t tail, uint32_t buf_len) {
    kv_circular_log_init(&self->log, storage, base, size, head, tail);
    for (size_t i = 0; i < 2; i++) {
        self->append_buf[i] = kv_storage_blk_alloc(self->log.storage, buf_len);
        self->read_buf[i] = kv_storage_blk_alloc(self->log.storage, buf_len);
    }
}

static void prefetch_cb(bool success, void *arg) {
    struct kv_index_log *self = arg;
    if (success) {
        self->is_prefetch_running = false;
        self->log.head = (self->log.head + self->buf_len) % self->log.size;
    } else {
        fprintf(stderr, "kv_index_log prefetch fail!");
        kv_circular_log_read(&self->log, self->log.head, PREFETCH_BUF(read, self), self->buf_len, prefetch_cb, self);
    }
}
uint64_t kv_index_log_get_offset_base(struct kv_index_log *self) {
    return ((self->log.size - self->buf_len + self->log.head) % self->log.size) << (7 + 8);
}
uint32_t *kv_index_log_read(struct kv_index_log *self) {  // return buf_len * KV_INDEX_LOG_ENTRY_PER_BLOCK entries
    assert(!self->is_prefetch_running);                   // TODO: may wait for prefetch task finish.
    self->read_buf_i = !self->read_buf_i;
    kv_circular_log_read(&self->log, self->log.head, PREFETCH_BUF(read, self), self->buf_len, prefetch_cb, self);
    self->is_prefetch_running = true;
    return BUF(read, self);
}

static void dump_cb(bool success, void *arg) {
    struct kv_index_log *self = arg;
    if (success) {
        self->is_dump_running = false;
        self->log.tail = (self->log.tail + self->buf_len) % self->log.size;
        kv_memset(PREFETCH_BUF(append, self), 0xFF, self->buf_len * KV_INDEX_LOG_ENTRY_PER_BLOCK * sizeof(uint32_t));
    } else {
        fprintf(stderr, "kv_index_log dump fail!");
        kv_circular_log_write(&self->log, self->log.tail, PREFETCH_BUF(append, self), self->buf_len, dump_cb, self);
    }
}

void kv_index_log_write(struct kv_index_log *self, uint64_t offset, uint32_t bucket_index) {
    uint64_t blk_i = (self->log.size - self->log.tail + OFFSET_TO_BLOCK(offset)) % self->log.size;
    assert(blk_i < 2 * self->buf_len);
    uint64_t i = OFFSET_TO_INDEX(offset);
    if (blk_i < self->buf_len) {
        BUF(append, self)[blk_i * KV_INDEX_LOG_ENTRY_PER_BLOCK + i] = bucket_index;
    } else {
        assert(!self->is_dump_running);  // TODO: may wait for dump task finish.
        kv_circular_log_write(&self->log, self->log.tail, PREFETCH_BUF(append, self), self->buf_len, dump_cb, self);
        self->is_dump_running = true;
        self->append_buf_i = !self->append_buf_i;
        BUF(append, self)[(blk_i - 1) * KV_INDEX_LOG_ENTRY_PER_BLOCK + i] = bucket_index;
    }
}
void kv_index_log_fini(struct kv_index_log *self) {
    for (size_t i = 0; i < 2; i++) {
        kv_storage_free(self->append_buf[i]);
        kv_storage_free(self->read_buf[i]);
    }
}