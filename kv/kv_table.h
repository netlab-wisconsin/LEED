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
void mehcached_table_reset(struct mehcached_table *self);
void mehcached_table_init(struct mehcached_table *self, struct kv_log *log, size_t num_buckets, bool concurrent_table_read, uint8_t extra_buckets_percentage);
void mehcached_table_free(struct mehcached_table *self);
#endif