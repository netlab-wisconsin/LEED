#ifndef _KV_TABLE_H_
#define _KV_TABLE_H_
#include "kv_log.h"
#define MEHCACHED_CONCURRENT
#define MEHCACHED_ITEMS_PER_BUCKET (15)  // 7 15 31

struct mehcached_bucket {
    uint32_t version;
    uint32_t next_extra_bucket_index;  // 1-base; 0 = no extra bucket
    uint64_t item_vec[MEHCACHED_ITEMS_PER_BUCKET];

    // 16: tag
    // 48: item offset (0 ~ 2^48-2)
    // item offset == MEHCACHED_ITEM_OFFSET_MASK: empty item

#define MEHCACHED_ITEM_TAG_MASK (((uint64_t)1 << 16) - 1)
#define MEHCACHED_ITEM_TAG(item_vec) ((item_vec) >> 48)
#define MEHCACHED_ITEM_OFFSET_MASK (((uint64_t)1 << 48) - 1)
#define MEHCACHED_ITEM_OFFSET(item_vec) ((item_vec)&MEHCACHED_ITEM_OFFSET_MASK)
#define MEHCACHED_ITEM_VEC(tag, item_offset) (((uint64_t)(tag) << 48) | (uint64_t)(item_offset))
#define MEHCACHED_ITEM_EMPTY(item_vec) (MEHCACHED_ITEM_OFFSET(item_vec) == MEHCACHED_ITEM_OFFSET_MASK)
#define MEHCACHED_ITEM_INIT_VAL UINT64_MAX
};

struct mehcached_table {
    struct kv_log *log; //(logs?)
    struct mehcached_bucket *buckets;
    struct mehcached_bucket *extra_buckets;  // = (buckets + num_buckets); extra_buckets[0] is not used because index 0 indicates "no more extra bucket"
    uint8_t concurrent_access_mode;
    uint32_t num_buckets;
    uint32_t num_buckets_mask;
    uint32_t num_extra_buckets;
    struct
    {
        uint32_t lock;
        uint32_t head;  // 1-base; 0 = no extra bucket
    } extra_bucket_free_list;
};

typedef kv_log_io_cb kv_table_op_cb ;
// 16 is from MEHCACHED_ITEM_TAG_MASK's log length
#define mehcached_calc_bucket_index(table,key_hash) ((uint32_t)(key_hash >> 16) & table->num_buckets_mask)
#define mehcached_calc_tag(key_hash) ((uint16_t)(key_hash & MEHCACHED_ITEM_TAG_MASK))

void mehcached_print_bucket(const struct mehcached_bucket *bucket);
void mehcached_print_buckets(const struct mehcached_table *table);

void mehcached_set(struct mehcached_table *self, uint64_t key_hash, uint8_t *key, size_t key_length, uint8_t *value,
                   size_t value_length, kv_table_op_cb cb, void *cb_arg);
void mehcached_table_reset(struct mehcached_table *self);
void mehcached_table_init(struct mehcached_table *self, struct kv_log *log, size_t num_buckets, bool concurrent_table_read, uint8_t extra_buckets_percentage);
void mehcached_table_free(struct mehcached_table *self);
#endif