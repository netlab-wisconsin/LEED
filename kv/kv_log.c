#include "kv_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/uio.h>

#include "memery_operation.h"
#define EMPTY_BUF_LEN(blk_size, length) ((blk_size) - (length - 1) % (blk_size)-1)

int kv_log_init(struct kv_log *self, struct storage *data_store, uint64_t head, uint64_t tail) {
    assert(data_store->block_size >= sizeof(struct kv_log_header));
    self->data_store = data_store;
    self->head = head;
    self->tail = tail;
    self->empty_buf = kv_malloc(self->data_store->block_size);
    return 0;
}

int kv_log_fini(struct kv_log *self) {
    kv_free(self->empty_buf);
    return 0;
}
struct kv_log_write_complete_arg {
    struct iovec iov[4];
    kv_log_io_cb cb;
    void *cb_arg;
};
#define kv_log_write_complete_arg_init(arg, cb, cb_arg) \
    do {                                                \
        arg->cb = cb;                                   \
        arg->cb_arg = cb_arg;                           \
    } while (0)

static void kv_log_write_complete(bool success, void *cb_arg) {
    struct kv_log_write_complete_arg *arg = cb_arg;
    if (arg->cb)
        arg->cb(success, arg->cb_arg);
    kv_free(arg->iov[0].iov_base);
    kv_free(arg);
}

static void _kv_log_write(struct kv_log *self, struct kv_log_header *header, uint8_t *key, uint8_t *value, kv_log_io_cb cb, void *cb_arg) {
    uint32_t length = sizeof(struct kv_log_header) + header->key_length + header->value_length;
    struct kv_log_write_complete_arg *arg = kv_malloc(sizeof(struct kv_log_write_complete_arg));
    kv_log_write_complete_arg_init(arg, cb, cb_arg);
    arg->iov[0] = (struct iovec){header, sizeof(struct kv_log_header)};
    arg->iov[1] = (struct iovec){key, header->key_length};
    arg->iov[2] = (struct iovec){value, header->value_length};
    arg->iov[3] = (struct iovec){self->empty_buf, EMPTY_BUF_LEN(self->data_store->block_size, length)};
    length += arg->iov[3].iov_len;
    storage_write(self->data_store, arg->iov, 4, self->tail, length, kv_log_write_complete, arg);
    self->tail += length;
}

void kv_log_write(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, uint8_t *value, uint32_t value_length,
                  kv_log_io_cb cb, void *cb_arg) {
    struct kv_log_header *header = kv_malloc(sizeof(struct kv_log_header));
    header->key_length = key_length;
    header->value_length = value_length;
    header->flag = KV_FLAG_DEFAULT & ~KV_DELETE_LOG_MASK;
    _kv_log_write(self, header, key, value, cb, cb_arg);
}

void kv_log_delete(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, kv_log_io_cb cb, void *cb_arg) {
    struct kv_log_header *header = kv_malloc(sizeof(struct kv_log_header));
    header->key_length = key_length;
    header->value_length = 0;
    header->flag = KV_FLAG_DEFAULT | KV_DELETE_LOG_MASK;
    _kv_log_write(self, header, key, NULL, cb, cb_arg);
}

void kv_log_read_header(struct kv_log *self, uint64_t offset, struct kv_log_header *header, kv_log_io_cb cb, void *cb_arg) {
    storage_read(self->data_store, header, 0, offset, self->data_store->block_size, cb, cb_arg);
}

void kv_log_read_value(struct kv_log *self, uint64_t offset, struct kv_log_header *header, uint8_t *value, kv_log_io_cb cb, void *cb_arg) {
    const uint32_t blk_size = self->data_store->block_size;
    uint32_t length = sizeof(struct kv_log_header) + header->key_length + header->value_length;
    if (length <= blk_size) {
        kv_memcpy(value, KV_LOG_VALUE(header), header->value_length);
        if (cb)
            cb(true, cb_arg);
        return;
    }
    kv_memcpy(value, KV_LOG_VALUE(header), blk_size);
    storage_read(self->data_store, value + blk_size, 0, offset + blk_size, length + EMPTY_BUF_LEN(blk_size, length) - blk_size, cb, cb_arg);
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
};

#define kv_log_read_cb_arg_init(arg, self, offset, key, key_length, value, value_length, cb, cb_arg, header) \
    do {                                                                                                     \
        arg->self = self;                                                                                    \
        arg->offset = offset;                                                                                \
        arg->key = key;                                                                                      \
        arg->key_length = key_length;                                                                        \
        arg->value = value;                                                                                  \
        arg->value_length = value_length;                                                                    \
        arg->cb = cb;                                                                                        \
        arg->cb_arg = cb_arg;                                                                                \
        arg->header = header;                                                                                \
    } while (0)

static void kv_log_read_value_cb(bool success, void *cb_arg) {
    struct kv_log_read_cb_arg *arg = cb_arg;
    if (arg->cb)
        arg->cb(success, arg->cb_arg);
    kv_free(arg->header);
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
    if (arg->value_length)
        *arg->value_length = arg->header->value_length;
    kv_log_read_value(arg->self, arg->offset, arg->header, arg->value, kv_log_read_value_cb, cb_arg);
    return;
fail:
    if (arg->cb)
        arg->cb(false, arg->cb_arg);
    kv_free(arg->header);
    kv_free(cb_arg);
}

void kv_log_read(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, uint8_t *value, uint32_t *value_length,
                 kv_log_io_cb cb, void *cb_arg) {
    struct kv_log_header *header = kv_malloc(self->data_store->block_size);
    struct kv_log_read_cb_arg *arg = kv_malloc(sizeof(struct kv_log_read_cb_arg));
    kv_log_read_cb_arg_init(arg, self, offset, key, key_length, value, value_length, cb, cb_arg, header);
    kv_log_read_header(self, offset, header, kv_log_read_header_cb, arg);
}