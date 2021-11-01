#ifndef _KV_DATA_STORE_H_
#define _KV_DATA_STORE_H_
#include "kv_bucket_log.h"
#include "kv_storage.h"
#include "kv_value_log.h"
struct kv_bucket_meta {
    uint8_t lock : 1;
    uint8_t chain_length : 7;
    uint32_t bucket_offset;
} __attribute__((packed));

struct kv_bucket_log_compact {
    struct kv_bucket *buffer[2];
    struct iovec *iov;
    uint32_t iovcnt;
    uint32_t offset, length, buf_len, i;
    struct {
        uint32_t err : 1;
        uint32_t running : 1;
        uint32_t io_cnt : 30;
    } state;
};
struct kv_data_store {
    struct kv_bucket_meta *bucket_meta;
    struct kv_bucket_log bucket_log;
    struct kv_value_log value_log;
    uint32_t bucket_num;
    uint32_t buckets_mask;
    uint32_t extra_bucket_num, allocated_bucket_num;
    uint8_t *bit_map;
    void *bucket_log_waitting_q;
    void *waiting_map;
    struct kv_bucket_log_compact compact;
};
typedef kv_storage_io_cb kv_data_store_cb;
void kv_data_store_init(struct kv_data_store *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets,
                        uint64_t value_log_block_num, uint32_t compact_buf_len, kv_data_store_cb cb, void *cb_arg);
void kv_data_store_fini(struct kv_data_store *self);
void kv_data_store_set(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t value_length,
                       kv_data_store_cb cb, void *cb_arg);
void kv_data_store_get(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t *value_length,
                       kv_data_store_cb cb, void *cb_arg);
#endif