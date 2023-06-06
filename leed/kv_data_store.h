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
    kv_bucket_key_set dirty_set;
    void *q;
    void *copy_ctx;
};
struct kv_data_store_copy_buf {
    uint32_t val_len;
    uint8_t *val_buf;
    void *ctx;
};
typedef void *kv_data_store_ctx;
typedef kv_storage_io_cb kv_data_store_cb;
typedef void (*kv_data_store_get_buf_cb)(uint8_t *key, uint8_t key_len, struct kv_data_store_copy_buf *buf, void *cb_arg);

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

static inline void kv_data_store_dirty(struct kv_data_store *self, uint8_t *key, uint8_t key_length) {
    kv_bucket_key_set_add(self->dirty_set, key, key_length);
}
static inline void kv_data_store_clean(struct kv_data_store *self, uint8_t *key, uint8_t key_length) {
    kv_bucket_key_set_del(self->dirty_set, key, key_length);
}
static inline bool kv_data_store_is_dirty(struct kv_data_store *self, uint8_t *key, uint8_t key_length) {
    return kv_bucket_key_set_find(self->dirty_set, key, key_length);
}

void kv_data_store_copy_commit(struct kv_data_store_copy_buf *buf);
bool kv_data_store_copy_forward(struct kv_data_store *self, uint8_t *key);
void kv_data_store_copy_range_counter(struct kv_data_store *self, uint8_t *key, bool inc);
void kv_data_store_copy_add_key_range(struct kv_data_store *self, uint8_t *start_key, uint8_t *end_key, kv_data_store_cb cb, void *cb_arg);
void kv_data_store_copy_del_key_range(struct kv_data_store *self, uint8_t *start_key, uint8_t *end_key, bool delete_items);
void kv_data_store_copy_init(struct kv_data_store *self, kv_data_store_get_buf_cb get_buf, void *arg, uint64_t buf_num, kv_data_store_cb cb);
void kv_data_store_copy_fini(struct kv_data_store *self);
#endif