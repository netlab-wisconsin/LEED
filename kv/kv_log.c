#include "kv_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/uio.h>

#include "memery_operation.h"


void kv_log_init(struct kv_log *self, struct kv_storage *storage, uint64_t head, uint64_t tail) {
    assert(storage->block_size >= sizeof(struct kv_log_header));
    self->storage = storage;
    self->head = head;
    self->tail = tail;
}

void kv_log_fini(struct kv_log *self) {}
struct kv_log_io_complete_arg {
    struct iovec iov[2];
    uint8_t *buf;
    kv_log_io_cb cb;
    void *cb_arg;
};
#define kv_log_io_complete_arg_init(arg, cb, cb_arg) \
    do {                                             \
        arg->cb = cb;                                \
        arg->cb_arg = cb_arg;                        \
    } while (0)

static void kv_log_io_complete(bool success, void *cb_arg) {
    struct kv_log_io_complete_arg *arg = cb_arg;
    if (arg->cb) arg->cb(success, arg->cb_arg);
    kv_storage_free(arg->buf);
    kv_free(arg);
}

void kv_log_write(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, uint8_t *value,
                  uint32_t value_length, kv_log_io_cb cb, void *cb_arg) {
    const uint32_t blk_size = self->storage->block_size;
    const uint32_t remain_val_len = value_length % blk_size;
    struct kv_log_io_complete_arg *arg = kv_malloc(sizeof(struct kv_log_io_complete_arg));
    kv_log_io_complete_arg_init(arg, cb, cb_arg);
    arg->buf = kv_storage_malloc(self->storage, blk_size);
    struct kv_log_header *header = (struct kv_log_header *)(arg->buf + blk_size - sizeof(struct kv_log_header));
    header->key_length = key_length;
    header->value_length = value_length;
    header->flag = KV_FLAG_DEFAULT & ~KV_DELETE_LOG_MASK;
    kv_memcpy(KV_LOG_KEY(header), key, key_length);
    if (sizeof(struct kv_log_header) + key_length + remain_val_len <= blk_size)
        kv_memcpy(arg->buf, value + value_length - remain_val_len, remain_val_len);
    else
        value_length += blk_size;
    arg->iov[0] = (struct iovec){value, value_length - remain_val_len};
    arg->iov[1] = (struct iovec){arg->buf, blk_size};
    const uint32_t write_len = value_length - remain_val_len + blk_size;
    kv_storage_write(self->storage, arg->iov, 2, self->tail, write_len, kv_log_io_complete, arg);
    self->tail += write_len;
}

void kv_log_delete(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, kv_log_io_cb cb, void *cb_arg) {
    const uint32_t blk_size = self->storage->block_size;
    struct kv_log_io_complete_arg *arg = kv_malloc(sizeof(struct kv_log_io_complete_arg));
    kv_log_io_complete_arg_init(arg, cb, cb_arg);
    arg->buf = kv_storage_malloc(self->storage, blk_size);
    struct kv_log_header *header = (struct kv_log_header *)(arg->buf + blk_size - sizeof(struct kv_log_header));
    header->key_length = key_length;
    header->value_length = 0;
    header->flag = KV_FLAG_DEFAULT | KV_DELETE_LOG_MASK;
    kv_memcpy(KV_LOG_KEY(header), key, key_length);
    kv_storage_write(self->storage, arg->buf, 0, self->tail, blk_size, kv_log_io_complete, arg);
    self->tail += blk_size;
}
uint64_t kv_log_get_offset(struct kv_log *self) { return self->tail - self->storage->block_size; }

void kv_log_read_header(struct kv_log *self, uint64_t offset, struct kv_log_header **header, void *buf, kv_log_io_cb cb,
                        void *cb_arg) {
    if (buf) {
        kv_storage_read(self->storage, buf, 0, offset, self->storage->block_size, cb, cb_arg);
        *header = (struct kv_log_header *)((uint8_t *)buf + self->storage->block_size - sizeof(struct kv_log_header));
    } else {
        struct kv_log_io_complete_arg *arg = kv_malloc(sizeof(struct kv_log_io_complete_arg));
        kv_log_io_complete_arg_init(arg, cb, cb_arg);
        arg->buf = kv_storage_malloc(self->storage, self->storage->block_size);
        kv_storage_read(self->storage, arg->buf, 0, offset, self->storage->block_size, kv_log_io_complete, arg);
        *header = (struct kv_log_header *)((uint8_t *)arg->buf + self->storage->block_size - sizeof(struct kv_log_header));
    }
}

void kv_log_read_value(struct kv_log *self, uint64_t offset, void *buf, uint8_t *value, kv_log_io_cb cb, void *cb_arg) {
    const uint32_t blk_size = self->storage->block_size;
    struct kv_log_header *header = (struct kv_log_header *)((uint8_t *)buf + blk_size - sizeof(struct kv_log_header));
    uint32_t value_length = header->value_length;
    const uint32_t remain_val_len = value_length % blk_size;
    if (sizeof(struct kv_log_header) + header->key_length + remain_val_len <= blk_size)
        kv_memcpy(value + value_length - remain_val_len, buf, remain_val_len);
    else
        value_length += blk_size;
    const uint32_t read_len = value_length - remain_val_len;
    if (read_len)
        kv_storage_read(self->storage, value, 0, offset - read_len, read_len, cb, cb_arg);
    else if (cb)
        cb(true, cb_arg);
}

struct kv_log_read_cb_arg {
    struct kv_log *self;
    uint64_t offset;
    uint8_t *key;
    uint16_t key_length;
    uint8_t *value;
    uint32_t *value_length;
    kv_log_io_cb cb;
    void *cb_arg;
    struct kv_log_header *header;
    void *buf;
};

#define kv_log_read_cb_arg_init(arg, self, offset, key, key_length, value, value_length, cb, cb_arg) \
    do {                                                                                             \
        arg->self = self;                                                                            \
        arg->offset = offset;                                                                        \
        arg->key = key;                                                                              \
        arg->key_length = key_length;                                                                \
        arg->value = value;                                                                          \
        arg->value_length = value_length;                                                            \
        arg->cb = cb;                                                                                \
        arg->cb_arg = cb_arg;                                                                        \
    } while (0)

static void kv_log_read_value_cb(bool success, void *cb_arg) {
    struct kv_log_read_cb_arg *arg = cb_arg;
    if (arg->cb) arg->cb(success, arg->cb_arg);
    kv_storage_free(arg->buf);
    kv_free(cb_arg);
}

static void kv_log_read_header_cb(bool success, void *cb_arg) {
    struct kv_log_read_cb_arg *arg = cb_arg;
    if (!success) {
        fprintf(stderr, "kv_log_read: io error.\n");
        goto fail;
    }
    if (arg->header->key_length != arg->key_length) {
        fprintf(stderr, "kv_log_read: different key length.\n");
        goto fail;
    }
    if (memcmp(KV_LOG_KEY(arg->header), arg->key, arg->key_length)) {
        fprintf(stderr, "kv_log_read: different key.\n");
        goto fail;
    }
    if (arg->value_length) *arg->value_length = arg->header->value_length;
    kv_log_read_value(arg->self, arg->offset, arg->buf, arg->value, kv_log_read_value_cb, cb_arg);
    return;
fail:
    if (arg->cb) arg->cb(false, arg->cb_arg);
    kv_storage_free(arg->buf);
    kv_free(cb_arg);
}

void kv_log_read(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, uint8_t *value,
                 uint32_t *value_length, kv_log_io_cb cb, void *cb_arg) {
    struct kv_log_read_cb_arg *arg = kv_malloc(sizeof(struct kv_log_read_cb_arg));
    kv_log_read_cb_arg_init(arg, self, offset, key, key_length, value, value_length, cb, cb_arg);
    arg->buf = kv_storage_malloc(self->storage, self->storage->block_size);
    kv_log_read_header(self, offset, &arg->header, arg->buf, kv_log_read_header_cb, arg);
}
