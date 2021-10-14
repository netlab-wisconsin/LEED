#include <assert.h>
#include <stdio.h>
#include "kv_table.h"
#include "kv_table_concurrent.h"
#include "memery_operation.h"


// 16 is from MEHCACHED_ITEM_TAG_MASK's log length
#define mehcached_calc_bucket_index(self,key_hash) ((uint32_t)(key_hash >> 16) & table->num_buckets_mask)

#define mehcached_calc_tag(key_hash) ((uint16_t)(key_hash & MEHCACHED_ITEM_TAG_MASK))

void mehcached_table_reset(struct mehcached_table *self) {
    for (size_t i = 0; i < self->num_buckets + self->num_extra_buckets; i++) {
        self->buckets[i].version = 0;
        self->buckets[i].next_extra_bucket_index = 0;
        kv_memset(self->buckets[i].item_vec, 0xFF, sizeof(uint64_t) * MEHCACHED_ITEMS_PER_BUCKET);
    }

    // initialize a free list of extra buckets
    if (self->num_extra_buckets == 0)
        self->extra_bucket_free_list.head = 0;  // no extra bucket at all
    else {
        uint32_t extra_bucket_index;
        for (extra_bucket_index = 1; extra_bucket_index < 1 + self->num_extra_buckets - 1; extra_bucket_index++)
            (self->extra_buckets + extra_bucket_index)->next_extra_bucket_index = extra_bucket_index + 1;
        (self->extra_buckets + extra_bucket_index)->next_extra_bucket_index = 0;  // no more free extra bucket

        self->extra_bucket_free_list.head = 1;
    }
}

void mehcached_table_init(struct mehcached_table *self, struct kv_log *log, size_t num_buckets, bool concurrent_table_read,
                          uint8_t extra_buckets_percentage) {
    assert((MEHCACHED_ITEMS_PER_BUCKET == 7 && sizeof(struct mehcached_bucket) == 64) ||
           (MEHCACHED_ITEMS_PER_BUCKET == 15 && sizeof(struct mehcached_bucket) == 128) ||
           (MEHCACHED_ITEMS_PER_BUCKET == 31 && sizeof(struct mehcached_bucket) == 256));
    assert(log);

    self->log = log;

    size_t log_num_buckets = 0;
    while (((size_t)1 << log_num_buckets) < num_buckets) log_num_buckets++;
    num_buckets = (size_t)1 << log_num_buckets;
    assert(log_num_buckets <= 32);

    self->concurrent_access_mode = (uint8_t)concurrent_table_read;

    self->num_buckets = (uint32_t)num_buckets;
    self->num_buckets_mask = (uint32_t)num_buckets - 1;
    self->num_extra_buckets = self->num_buckets * extra_buckets_percentage / 100;

    self->buckets = kv_calloc(self->num_buckets + self->num_extra_buckets, sizeof(struct mehcached_bucket));
    self->extra_buckets = self->buckets + self->num_buckets - 1;  // subtract by one to compensate 1-base indices
    // the rest extra_bucket information is initialized in mehcached_table_reset()

    mehcached_table_reset(self);
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("!NDEBUG (low performance)\n");
#endif

#ifdef MEHCACHED_VERBOSE
    printf("MEHCACHED_VERBOSE (low performance)\n");
#endif

#ifdef MEHCACHED_CONCURRENT
    printf("MEHCACHED_CONCURRENT (low performance)\n");
#endif

    printf("num_buckets = %u\n", self->num_buckets);
    printf("num_extra_buckets = %u\n", self->num_extra_buckets);
    printf("\n");
}

void mehcached_table_free(struct mehcached_table *self)
{
    assert(self);
    kv_free(self->buckets);
}