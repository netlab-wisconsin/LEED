#ifndef _KV_LOG_H_
#define _KV_LOG_H_
#include "storage.h"
struct kv_log_header {
    uint16_t flag;
    uint16_t key_length;
    uint32_t value_length;
    uint8_t data[0];
#define KV_FLAG_DEFAULT ((uint16_t)0x8000)
#define KV_DELETE_LOG_MASK ((uint16_t)1)
#define KV_DELETE_LOG(header) ((header)->flag & KV_DELETE_LOG_MASK)
#define KV_LOG_KEY(header) ((header)->data)
#define KV_LOG_VALUE(header) ((header)->data + (header)->key_length)
};
struct kv_log {
    uint64_t head, tail;
    struct storage *data_store;
    uint8_t *empty_buf;
};

typedef storage_io_completion_cb kv_log_io_cb;

int kv_log_init(struct kv_log *self, struct storage *data_store, uint64_t head, uint64_t tail);
int kv_log_fini(struct kv_log *self);
void kv_log_write(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, uint8_t *value, uint32_t value_length,
                  kv_log_io_cb cb, void *cb_arg);
void kv_log_delete(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, kv_log_io_cb cb, void *cb_arg);
void kv_log_read_header(struct kv_log *self, uint64_t offset, struct kv_log_header *header, kv_log_io_cb cb, void *cb_arg);
void kv_log_read_value(struct kv_log *self, uint64_t offset, struct kv_log_header *header, uint8_t *value, kv_log_io_cb cb, void *cb_arg);
void kv_log_read(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, uint8_t *value, uint32_t *value_length,
                 kv_log_io_cb cb, void *cb_arg);
#endif