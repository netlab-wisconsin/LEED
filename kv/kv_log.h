#ifndef _KV_LOG_H_
#define _KV_LOG_H_
#include "kv_storage.h"
struct kv_log_header {
    uint32_t value_length;
    uint16_t key_length;
    uint16_t flag;

#define KV_FLAG_DEFAULT ((uint16_t)0x8000)
#define KV_DELETE_LOG_MASK ((uint16_t)1)
#define KV_DELETE_LOG(header_ptr) ((header_ptr)->flag & KV_DELETE_LOG_MASK)
#define KV_LOG_KEY(header_ptr) ((uint8_t *)(header_ptr)-(header_ptr)->key_length)
// #define KV_LOG_VALUE(header_ptr) ((header_ptr)->data + (header_ptr)->key_length)
};
/*
|                block 0               | block 1 | block 2 | 
| <header> <key> <value> <empty space> | <value> | <value> |
or 
|         block 0        |        block 1        | block 2 | 
| <header> <key> <value> | <value> <empty space> | <value> |

| block 0 |                block 1               |
| <value> | <value> <empty space> <key> <header> |
or 
| block 0 |         block 1       |            block 2           |
| <value> | <value> <empty space> | <empty space> <key> <header> |
*/
struct kv_log {
    uint64_t head, tail;
    struct kv_storage *storage;
};

typedef kv_storage_io_cb kv_log_io_cb;

void kv_log_init(struct kv_log *self, struct kv_storage *storage, uint64_t head, uint64_t tail);
void kv_log_fini(struct kv_log *self);
void kv_log_write(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, uint8_t *value,
                  uint32_t value_length, kv_log_io_cb cb, void *cb_arg);
void kv_log_delete(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, kv_log_io_cb cb, void *cb_arg);
uint64_t kv_log_get_offset(struct kv_log *self);
void kv_log_read_header(struct kv_log *self, uint64_t offset, struct kv_log_header **header, void *buf, kv_log_io_cb cb,
                        void *cb_arg);
void kv_log_read_value(struct kv_log *self, uint64_t offset, void *buf, uint8_t *value, kv_log_io_cb cb, void *cb_arg);
void kv_log_read(struct kv_log *self, uint64_t offset, uint8_t *key, uint16_t key_length, uint8_t *value,
                 uint32_t *value_length, kv_log_io_cb cb, void *cb_arg);
#endif