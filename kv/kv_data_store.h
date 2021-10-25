#ifndef _KV_DATA_STORE_H_
#define _KV_DATA_STORE_H_
#include "kv_bucket_log.h"
#include "kv_value_log.h"
#include "kv_storage.h"
struct kv_bucket_offset {
    uint32_t flag : 1;
    uint32_t bucket_offset : 31;
};
struct kv_data_store {
    struct kv_bucket_offset *bucket_offsets;
    struct kv_bucket_log bucket_log;
    struct kv_value_log value_log;
    uint32_t main_bucket_num;
    uint32_t extra_bucket_num;
    uint32_t buckets_mask;
    uint8_t *bit_map;
    uint32_t free_list_head;
    struct kv_bucket *compact_buffer;
    void *waiting_map;
};
typedef  kv_storage_io_cb kv_data_store_cb;
void kv_data_store_init(struct kv_data_store *self, struct kv_storage *storage, uint32_t base, uint32_t size,
                        kv_data_store_cb cb, void *cb_arg);
void kv_data_store_fini(struct kv_data_store *self);
void kv_data_store_set(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t value_length,
                       kv_data_store_cb cb, void *cb_arg);
void kv_data_store_get(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t *value_length,
                       kv_data_store_cb cb, void *cb_arg);
#endif