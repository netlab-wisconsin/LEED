// Copyright 2014 Carnegie Mellon University
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "common.h"
#include "fawnds_flash.h"

MEHCACHED_BEGIN

#define MEHCACHED_MAX_KEY_LENGTH (255)
#define MEHCACHED_MAX_VALUE_LENGTH (1048575)


#ifndef MEHCACHED_NO_EVICTION
// #define MEHCACHED_ITEMS_PER_BUCKET (7)
#define MEHCACHED_ITEMS_PER_BUCKET (15)
#else
#define MEHCACHED_ITEMS_PER_BUCKET (7)
// #define MEHCACHED_ITEMS_PER_BUCKET (15)
#endif

// do move-to-head if when (item's distance from tail) >= (pool size) * mth_threshold
// 0.0: full LRU; 1.0: full FIFO
#define MEHCACHED_MTH_THRESHOLD_FIFO (1.0)
#define MEHCACHED_MTH_THRESHOLD_LRU (0.0)

#define MEHCACHED_SINGLE_ALLOC

#ifdef MEHCACHED_COLLECT_STATS
#define MEHCACHED_STAT_INC(table, name) do { __sync_add_and_fetch(&(table)->stats.name, 1); } while (0)
#define MEHCACHED_STAT_DEC(table, name) do { __sync_sub_and_fetch(&(table)->stats.name, 1); } while (0)
#else
#define MEHCACHED_STAT_INC(table, name) do { (void)table; } while (0)
#define MEHCACHED_STAT_DEC(table, name) do { (void)table; } while (0)
#endif


struct mehcached_bucket
{
    uint32_t version;   // XXX: is uint32_t wide enough?
    uint32_t next_extra_bucket_index;   // 1-base; 0 = no extra bucket
    uint64_t item_vec[MEHCACHED_ITEMS_PER_BUCKET];

    // 16: tag (1-base)
    // 48: item offset
    // item == 0: empty item

    #define MEHCACHED_TAG_MASK (((uint64_t)1 << 16) - 1)
    #define MEHCACHED_TAG(item_vec) ((item_vec) >> 48)
    #define MEHCACHED_ITEM_OFFSET_MASK (((uint64_t)1 << 48) - 1)
    #define MEHCACHED_ITEM_OFFSET(item_vec) ((item_vec) & MEHCACHED_ITEM_OFFSET_MASK)
    #define MEHCACHED_ITEM_VEC(tag, item_offset) (((uint64_t)(tag) << 48) | (uint64_t)(item_offset))

};



#define MEHCACHED_MAX_POOLS (16)

struct mehcached_table
{
    fawn::FawnDS_Flash *ds=NULL;
    struct mehcached_bucket *buckets;
    struct mehcached_bucket *extra_buckets; // = (buckets + num_buckets); extra_buckets[0] is not used because index 0 indicates "no more extra bucket"

    uint8_t concurrent_access_mode;

    uint32_t num_buckets;
    uint32_t num_buckets_mask;
    uint32_t num_extra_buckets;

    struct
    {
        uint32_t lock;
        uint32_t head;   // 1-base; 0 = no extra bucket
    } extra_bucket_free_list MEHCACHED_ALIGNED(64);

    uint8_t rshift;

#ifdef MEHCACHED_COLLECT_STATS
    struct
    {
        size_t count;
        size_t set_nooverwrite;
        size_t set_new;
        size_t set_inplace;
        size_t set_evicted;
        size_t get_found;
        size_t get_notfound;
        size_t test_found;
        size_t test_notfound;
        size_t delete_found;
        size_t delete_notfound;
        size_t cleanup;
        size_t move_to_head_performed;
        size_t move_to_head_skipped;
        size_t move_to_head_failed;
    } stats;
#endif
} MEHCACHED_ALIGNED(64);


static
void
mehcached_print_bucket(const struct mehcached_bucket *bucket);

void
mehcached_print_buckets(const struct mehcached_table *table);

static
void
mehcached_print_stats(const struct mehcached_table *table);

static
void
mehcached_reset_table_stats(struct mehcached_table *table);

static
uint32_t
mehcached_calc_bucket_index(const struct mehcached_table *table, uint64_t key_hash);

static
uint16_t
mehcached_calc_tag(uint64_t key_hash);


static
bool
mehcached_compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len);


bool
mehcached_get(struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length, string &value);

static
bool
mehcached_test(uint8_t current_alloc_id, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length);

bool
mehcached_set(struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length);

bool
mehcached_delete(struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length);


void
mehcached_table_reset(struct mehcached_table *table);

void
mehcached_table_init(struct mehcached_table *table, size_t num_buckets, bool concurrent_table_read, bool concurrent_table_write,const char * filename,uint8_t extra_buckets_percentage);

void
mehcached_table_free(struct mehcached_table *table);

MEHCACHED_END

