#ifndef _KV_DATA_STORE_H_
#define _KV_DATA_STORE_H_
#include "kv_bucket_log.h"
#include "kv_ds_queue.h"
#include "kv_storage.h"
#include "kv_value_log.h"
struct kv_data_store {
    struct kv_bucket_log bucket_log;
    struct kv_value_log value_log;
    struct kv_ds_queue *ds_queue;
    uint32_t ds_id;
    uint64_t log_bucket_num;  // cluster
    void *q;
};
struct kv_data_store_get_range_buf {
    uint8_t *key;
    uint8_t *key_length;
    uint8_t *value;
    uint32_t *value_length;
};
typedef void (*kv_data_store_range_cb)(struct kv_data_store_get_range_buf *buf, void *cb_arg);
typedef void *kv_data_store_ctx;
typedef kv_storage_io_cb kv_data_store_cb;
void kv_data_store_init(struct kv_data_store *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets, uint64_t log_bucket_num,
                        uint64_t value_log_block_num, uint32_t compact_buf_len, struct kv_ds_queue *ds_queue, uint32_t ds_id);
void kv_data_store_fini(struct kv_data_store *self);
kv_data_store_ctx kv_data_store_set(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t value_length,
                                    kv_data_store_cb cb, void *cb_arg);
void kv_data_store_set_commit(kv_data_store_ctx arg, bool success);
void kv_data_store_get(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t *value_length,
                       kv_data_store_cb cb, void *cb_arg);
kv_data_store_ctx kv_data_store_delete(struct kv_data_store *self, uint8_t *key, uint8_t key_length, kv_data_store_cb cb, void *cb_arg);
void kv_data_store_del_commit(kv_data_store_ctx arg, bool success);

void kv_data_store_get_range(struct kv_data_store *self, uint8_t *start_key, uint8_t *end_key, uint64_t buf_num,
                             kv_data_store_range_cb get_buf, void *arg, kv_data_store_range_cb get_range_cb, void *cb_arg);
void kv_data_store_del_range(struct kv_data_store *self, uint8_t *start_key, uint8_t *end_key);
#endif