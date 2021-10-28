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

struct kv_data_store {
    struct kv_bucket_meta *bucket_meta;
    struct kv_bucket_log bucket_log;
    struct kv_value_log value_log;
    uint32_t bucket_num;
    uint32_t buckets_mask;
    uint32_t extra_bucket_num, allocated_bucket_num;
    uint8_t *bit_map;
    void *private_data;
    void *waiting_map;
#define COMPACT_BUCKET_NUM 256
    struct kv_bucket *compact_buffer;
    struct iovec compact_iov[COMPACT_BUCKET_NUM];
    uint32_t compact_iovcnt, compact_offset, compact_length;
    bool is_compact_task_running;
};
typedef kv_storage_io_cb kv_data_store_cb;
void kv_data_store_init(struct kv_data_store *self, struct kv_storage *storage, uint32_t base, uint32_t num_buckets,
                        uint64_t value_log_block_num, kv_data_store_cb cb, void *cb_arg);
void kv_data_store_fini(struct kv_data_store *self);
void kv_data_store_set(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t value_length,
                       kv_data_store_cb cb, void *cb_arg);
void kv_data_store_get(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t *value_length,
                       kv_data_store_cb cb, void *cb_arg);
#endif