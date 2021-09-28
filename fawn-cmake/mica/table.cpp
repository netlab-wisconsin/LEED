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


#include "table.h"
#include "util.h"

#include <stdio.h>


// a test feature to deduplicate PUT requests within the same batch
//#define MEHCACHED_DEDUP_WITHIN_BATCH

static
void
mehcached_print_bucket(const struct mehcached_bucket *bucket)
{
    printf("<bucket %zx>\n", (size_t)bucket);
    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        printf("  item_vec[%zu]: tag=%lu, item_offset=%lu\n", item_index, MEHCACHED_TAG(bucket->item_vec[item_index]),MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
}

static
void
mehcached_print_buckets(const struct mehcached_table *table)
{
    size_t bucket_index;
    for (bucket_index = 0; bucket_index < table->num_buckets; bucket_index++)
    {
        struct mehcached_bucket *bucket = table->buckets + bucket_index;
        mehcached_print_bucket(bucket);
    }
    printf("\n");
}

static
void
mehcached_print_stats(const struct mehcached_table *table MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_COLLECT_STATS
    printf("count:                  %10zu\n", table->stats.count);
    printf("set_nooverwrite:        %10zu | ", table->stats.set_nooverwrite);
    printf("set_new:                %10zu | ", table->stats.set_new);
    printf("set_inplace:            %10zu | ", table->stats.set_inplace);
    printf("set_evicted:            %10zu\n", table->stats.set_evicted);
    printf("get_found:              %10zu | ", table->stats.get_found);
    printf("get_notfound:           %10zu\n", table->stats.get_notfound);
    printf("test_found:             %10zu | ", table->stats.test_found);
    printf("test_notfound:          %10zu\n", table->stats.test_notfound);
    printf("cleanup:                %10zu\n", table->stats.cleanup);
    printf("move_to_head_performed: %10zu | ", table->stats.move_to_head_performed);
    printf("move_to_head_skipped:   %10zu | ", table->stats.move_to_head_skipped);
    printf("move_to_head_failed:    %10zu\n", table->stats.move_to_head_failed);
#endif
}

static
void
mehcached_reset_table_stats(struct mehcached_table *table MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_COLLECT_STATS
    size_t count = table->stats.count;
    memset(&table->stats, 0, sizeof(table->stats));
    table->stats.count = count;
#endif
}

static
uint32_t
mehcached_calc_bucket_index(const struct mehcached_table *table, uint64_t key_hash)
{
    // 16 is from MEHCACHED_TAG_MASK's log length
    return (uint32_t)(key_hash >> 16) & table->num_buckets_mask;
}

static
uint16_t
mehcached_calc_tag(uint64_t key_hash)
{
    uint16_t tag = (uint16_t)(key_hash & MEHCACHED_TAG_MASK);
    if (tag == 0)
        return 1;
    else
        return tag;
}

static
// uint64_t
uint32_t
mehcached_read_version_begin(const struct mehcached_table *table MEHCACHED_UNUSED, const struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode != 0)
    {
        while (true)
        {
            // uint64_t v = *(volatile uint64_t *)&bucket->version;
            uint32_t v = *(volatile uint32_t *)&bucket->version;
            memory_barrier();
            // if ((v & 1UL) != 0UL)
            if ((v & 1U) != 0U)
                continue;
            return v;
        }
    }
    else
        // return 0UL;
        return 0U;
#else
    // return 0UL;
    return 0U;
#endif
}

static
//uint64_t
uint32_t
mehcached_read_version_end(const struct mehcached_table *table MEHCACHED_UNUSED, const struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode != 0)
    {
        memory_barrier();
        // uint64_t v = *(volatile uint64_t *)&bucket->version;
        uint32_t v = *(volatile uint32_t *)&bucket->version;
        return v;
    }
    else
        // return 0UL;
        return 0U;
#else
    // return 0UL;
    return 0U;
#endif
}

static
void
mehcached_lock_bucket(const struct mehcached_table *table MEHCACHED_UNUSED, struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode == 1)
    {
        // assert((*(volatile uint64_t *)&bucket->version & 1UL) == 0UL);
        // (*(volatile uint64_t *)&bucket->version)++;
        assert((*(volatile uint32_t *)&bucket->version & 1U) == 0U);
        (*(volatile uint32_t *)&bucket->version)++;
        memory_barrier();
    }
    else if (table->concurrent_access_mode == 2)
    {
        while (1)
        {
            // uint64_t v = *(volatile uint64_t *)&bucket->version & ~1UL;
            uint32_t v = *(volatile uint32_t *)&bucket->version & ~1U;
            // uint64_t new_v = v | 1UL;
            uint32_t new_v = v | 1U;
            // if (__sync_bool_compare_and_swap((volatile uint64_t *)&bucket->version, v, new_v))
            if (__sync_bool_compare_and_swap((volatile uint32_t *)&bucket->version, v, new_v))
                break;
        }
    }
#endif
}

static
void
mehcached_unlock_bucket(const struct mehcached_table *table MEHCACHED_UNUSED, struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode != 0)
    {
        memory_barrier();
        // assert((*(volatile uint64_t *)&bucket->version & 1UL) == 1UL);
        assert((*(volatile uint32_t *)&bucket->version & 1U) == 1U);
        // no need to use atomic add because this thread is the only one writing to version
        // (*(volatile uint64_t *)&bucket->version)++;
        (*(volatile uint32_t *)&bucket->version)++;
    }
#endif
}

static
void
mehcached_lock_extra_bucket_free_list(struct mehcached_table *table)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode == 2)
    {
        while (1)
        {
            if (__sync_bool_compare_and_swap((volatile uint32_t *)&table->extra_bucket_free_list.lock, 0U, 1U))
                break;
        }
    }
#endif
}

static
void
mehcached_unlock_extra_bucket_free_list(struct mehcached_table *table)
{
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode == 2)
    {
        memory_barrier();
        assert((*(volatile uint32_t *)&table->extra_bucket_free_list.lock & 1U) == 1U);
        // no need to use atomic add because this thread is the only one writing to version
        *(volatile uint32_t *)&table->extra_bucket_free_list.lock = 0U;
    }
#endif
}

static
bool
mehcached_has_extra_bucket(struct mehcached_bucket *bucket MEHCACHED_UNUSED)
{
#ifndef MEHCACHED_NO_EVICTION
    return false;
#else
    return bucket->next_extra_bucket_index != 0;
#endif
}

static
struct mehcached_bucket *
mehcached_extra_bucket(const struct mehcached_table *table, uint32_t extra_bucket_index)
{
    // extra_bucket_index is 1-base
    assert(extra_bucket_index != 0);
    assert(extra_bucket_index < 1 + table->num_extra_buckets);
    return table->extra_buckets + extra_bucket_index;   // extra_buckets[1] is the actual start
}

static
bool
mehcached_alloc_extra_bucket(struct mehcached_table *table, struct mehcached_bucket *bucket)
{
    assert(!mehcached_has_extra_bucket(bucket));

    mehcached_lock_extra_bucket_free_list(table);

    if (table->extra_bucket_free_list.head == 0)
    {
        mehcached_unlock_extra_bucket_free_list(table);
        return false;
    }

    // take the first free extra bucket
    uint32_t extra_bucket_index = table->extra_bucket_free_list.head;
    table->extra_bucket_free_list.head = mehcached_extra_bucket(table, extra_bucket_index)->next_extra_bucket_index;
    mehcached_extra_bucket(table, extra_bucket_index)->next_extra_bucket_index = 0;

    // add it to the given bucket
    // concurrent readers may see the new extra_bucket from this point
    bucket->next_extra_bucket_index = extra_bucket_index;

    mehcached_unlock_extra_bucket_free_list(table);
    return true;
}

static
void
mehcached_free_extra_bucket(struct mehcached_table *table, struct mehcached_bucket *bucket)
{
    assert(mehcached_has_extra_bucket(bucket));

    uint32_t extra_bucket_index = bucket->next_extra_bucket_index;

    struct mehcached_bucket *extra_bucket = mehcached_extra_bucket(table, extra_bucket_index);
    assert(extra_bucket->next_extra_bucket_index == 0); // only allows freeing the tail of the extra bucket chain

    // verify if the extra bucket is empty (debug only)
    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        assert(extra_bucket->item_vec[item_index] == 0);

    // detach
    bucket->next_extra_bucket_index = 0;

    // add freed extra bucket to the free list
    mehcached_lock_extra_bucket_free_list(table);

    extra_bucket->next_extra_bucket_index = table->extra_bucket_free_list.head;
    table->extra_bucket_free_list.head = extra_bucket_index;

    mehcached_unlock_extra_bucket_free_list(table);
}

static
void
mehcached_fill_hole(struct mehcached_table *table, struct mehcached_bucket *bucket, size_t unused_item_index)
{
    // there is no extra bucket; do not try to fill a hole within the same bucket
    if (!mehcached_has_extra_bucket(bucket))
        return;

    struct mehcached_bucket *prev_extra_bucket = NULL;
    struct mehcached_bucket *current_extra_bucket = bucket;
    while (mehcached_has_extra_bucket(current_extra_bucket) != 0)
    {
        prev_extra_bucket = current_extra_bucket;
        current_extra_bucket = mehcached_extra_bucket(table, current_extra_bucket->next_extra_bucket_index);
    }

    bool last_item = true;
    size_t moved_item_index;

    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        if (current_extra_bucket->item_vec[item_index] != 0)
        {
            moved_item_index = item_index;
            break;
        }

    for (item_index++; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        if (current_extra_bucket->item_vec[item_index] != 0)
        {
            last_item = false;
            break;
        }

    // move the entry
    bucket->item_vec[unused_item_index] = current_extra_bucket->item_vec[moved_item_index];
    current_extra_bucket->item_vec[moved_item_index] = 0;

    // if it was the last entry of current_extra_bucket, free current_extra_bucket
    if (last_item)
        mehcached_free_extra_bucket(table, prev_extra_bucket);
}

static
size_t
mehcached_find_empty(struct mehcached_table *table, struct mehcached_bucket *bucket, struct mehcached_bucket **located_bucket)
{
    struct mehcached_bucket *current_bucket = bucket;
    while (true)
    {
        size_t item_index;
        for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        {
            if (current_bucket->item_vec[item_index] == 0)
            {
                *located_bucket = current_bucket;
                return item_index;
            }
        }
        if (!mehcached_has_extra_bucket(current_bucket))
            break;
        current_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
    }

    // no space; alloc new extra_bucket
    if (mehcached_alloc_extra_bucket(table, current_bucket))
    {
        *located_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
        return 0;   // use the first slot (it should be empty)
    }
    else
    {
        // no free extra_bucket
        *located_bucket = NULL;
        return MEHCACHED_ITEMS_PER_BUCKET;
    }
}




static
bool
mehcached_compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len)
{
    return key1_len == key2_len && mehcached_memcmp8(key1, key2, key1_len);
}

static
size_t
mehcached_find_item_index(const struct mehcached_table *table, struct mehcached_bucket *bucket, uint16_t tag, const uint8_t *key, size_t key_length, struct mehcached_bucket **located_bucket,uint32_t *data_len= nullptr)
{
    struct mehcached_bucket *current_bucket = bucket;
    fawn::DataHeader data_header{};
    string input_key;
    while (true)
    {
        size_t item_index;
        for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        {
            if (MEHCACHED_TAG(current_bucket->item_vec[item_index]) != tag)
                continue;

            // we may read garbage values, which do not cause any fatal issue

            table->ds->ReadIntoHeader(MEHCACHED_ITEM_OFFSET(current_bucket->item_vec[item_index]), data_header, input_key);

            // a key comparison reads up to min(source key length and destination key length), which is always safe to do
            if (!mehcached_compare_keys(reinterpret_cast<const uint8_t *>(input_key.c_str()), data_header.key_length, key, key_length))
                continue;

            // we skip any validity check because it will be done by callers who are doing more jobs with this result

    #ifdef MEHCACHED_VERBOSE
            printf("find item index: %zu\n", item_index);
    #endif
            *located_bucket = current_bucket;
            if(data_len!= nullptr)  *data_len=data_header.data_length;
            return item_index;
        }

        if (!mehcached_has_extra_bucket(current_bucket))
            break;
        current_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
    }

#ifdef MEHCACHED_VERBOSE
    printf("could not find item index\n");
#endif

    *located_bucket = NULL;
    return MEHCACHED_ITEMS_PER_BUCKET;
}

static
size_t
mehcached_find_same_tag(const struct mehcached_table *table, struct mehcached_bucket *bucket, uint16_t tag, struct mehcached_bucket **located_bucket)
{
    struct mehcached_bucket *current_bucket = bucket;

    while (true)
    {
        size_t item_index;
        for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        {
            if (MEHCACHED_TAG(current_bucket->item_vec[item_index]) != tag)
                continue;

            *located_bucket = current_bucket;
            return item_index;
        }

        if (!mehcached_has_extra_bucket(current_bucket))
            break;
        current_bucket = mehcached_extra_bucket(table, current_bucket->next_extra_bucket_index);
    }

    *located_bucket = NULL;
    return MEHCACHED_ITEMS_PER_BUCKET;
}


bool
mehcached_get(struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length, string &value)
{
    assert(key_length <= MEHCACHED_MAX_KEY_LENGTH);

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);
    uint16_t tag = mehcached_calc_tag(key_hash);

    struct mehcached_bucket *bucket = table->buckets + bucket_index;

    while (true)
    {
        uint32_t version_start = mehcached_read_version_begin(table, bucket);

        struct mehcached_bucket *located_bucket;
        uint32_t value_length;
        size_t item_index = mehcached_find_item_index(table, bucket, tag, key, key_length, &located_bucket,&value_length);
        if (item_index == MEHCACHED_ITEMS_PER_BUCKET)
        {
            if (version_start != mehcached_read_version_end(table, bucket))
                continue;
            MEHCACHED_STAT_INC(table, get_notfound);
            return false;
        }

        uint64_t item_vec = located_bucket->item_vec[item_index];
        uint64_t item_offset = MEHCACHED_ITEM_OFFSET(item_vec);
        if(!table->ds->ReadData(item_offset,key_length,value_length,value)){
            if (version_start != mehcached_read_version_end(table, bucket))
                continue;
            MEHCACHED_STAT_INC(table, get_notfound);
            return false;
        }
        
        if (version_start != mehcached_read_version_end(table, bucket))
            continue;

        // the following is optional processing (we will return the value retrieved above)

        MEHCACHED_STAT_INC(table, get_found);        
        break;
    }

    return true;
}

static
bool
mehcached_test(uint8_t current_alloc_id MEHCACHED_UNUSED, struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length)
{
    assert(key_length <= MEHCACHED_MAX_KEY_LENGTH);

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);
    uint16_t tag = mehcached_calc_tag(key_hash);

    struct mehcached_bucket *bucket = table->buckets + bucket_index;

    while (true)
    {
        uint32_t version_start = mehcached_read_version_begin(table, bucket);

        struct mehcached_bucket *located_bucket;
        size_t item_index = mehcached_find_item_index(table, bucket, tag, key, key_length, &located_bucket);

        if (version_start != mehcached_read_version_end(table, bucket))
            continue;

        if (item_index != MEHCACHED_ITEMS_PER_BUCKET)
        {
            MEHCACHED_STAT_INC(table, test_found);
            return true;
        }
        else
        {
            MEHCACHED_STAT_INC(table, test_notfound);
            return false;
        }
    }

    // not reachable
    return false;
}


bool
mehcached_set(struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length, const uint8_t *value, size_t value_length)
{
    assert(key_length <= MEHCACHED_MAX_KEY_LENGTH);
    assert(value_length <= MEHCACHED_MAX_VALUE_LENGTH);

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);
    uint16_t tag = mehcached_calc_tag(key_hash);

    struct mehcached_bucket *bucket = table->buckets + bucket_index;
    mehcached_lock_bucket(table, bucket);

    struct mehcached_bucket *located_bucket;
    size_t item_index = mehcached_find_item_index(table, bucket, tag, key, key_length, &located_bucket);
    if (item_index == MEHCACHED_ITEMS_PER_BUCKET)
    {
        item_index = mehcached_find_empty(table, bucket, &located_bucket);
        if (item_index == MEHCACHED_ITEMS_PER_BUCKET)
        {
            // no more space
            // TODO: add a statistics entry
            mehcached_unlock_bucket(table, bucket);
            return false;
        }
    }
    uint64_t item_offset = table->ds->GetTail();

    MEHCACHED_STAT_INC(table, set_new);
    if(!table->ds->Write(reinterpret_cast<const char *>(key), key_length, reinterpret_cast<const char *>(value), value_length, item_offset)){
        mehcached_unlock_bucket(table, bucket);
        return false;
    }

    if (located_bucket->item_vec[item_index] != 0)
    {
        MEHCACHED_STAT_INC(table, set_evicted);
        MEHCACHED_STAT_DEC(table, count);
    }

    located_bucket->item_vec[item_index] = MEHCACHED_ITEM_VEC(tag, item_offset);

    mehcached_unlock_bucket(table, bucket);
    MEHCACHED_STAT_INC(table, count);

    return true;
}


bool
mehcached_delete(struct mehcached_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length)
{
    assert(key_length <= MEHCACHED_MAX_KEY_LENGTH);

    uint32_t bucket_index = mehcached_calc_bucket_index(table, key_hash);
    uint16_t tag = mehcached_calc_tag(key_hash);

    struct mehcached_bucket *bucket = table->buckets + bucket_index;

    mehcached_lock_bucket(table, bucket);

    struct mehcached_bucket *located_bucket;
    size_t item_index = mehcached_find_item_index(table, bucket, tag, key, key_length, &located_bucket);
    if (item_index == MEHCACHED_ITEMS_PER_BUCKET)
    {
        mehcached_unlock_bucket(table, bucket);
        MEHCACHED_STAT_INC(table, delete_notfound);
        return false;
    }
    if(!table->ds->Delete(reinterpret_cast<const char *>(key), key_length, MEHCACHED_ITEM_OFFSET(located_bucket->item_vec[item_index]))){
        mehcached_unlock_bucket(table, bucket);
        return false;
    }
    located_bucket->item_vec[item_index] = 0;
    MEHCACHED_STAT_DEC(table, count);

    mehcached_fill_hole(table, located_bucket, item_index);

    mehcached_unlock_bucket(table, bucket);

    MEHCACHED_STAT_INC(table, delete_found);
    return true;
}




void
mehcached_table_reset(struct mehcached_table *table)
{
    size_t bucket_index;
#ifdef MEHCACHED_ALLOC_DYNAMIC
    mehcached_dynamic_lock(&table->alloc);
#endif
    for (bucket_index = 0; bucket_index < table->num_buckets; bucket_index++)
    {
        struct mehcached_bucket *bucket = table->buckets + bucket_index;
        size_t item_index;
        for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
            if (bucket->item_vec[item_index] != 0)
            {
#ifdef MEHCACHED_ALLOC_MALLOC
                mehcached_malloc_deallocate(&table->alloc, MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
#endif
#ifdef MEHCACHED_ALLOC_DYNAMIC
                mehcached_dynamic_deallocate(&table->alloc, MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
#endif
            }
    }
#ifdef MEHCACHED_ALLOC_DYNAMIC
    mehcached_dynamic_unlock(&table->alloc);
#endif

    memset(table->buckets, 0, sizeof(struct mehcached_bucket) * (table->num_buckets + table->num_extra_buckets));

    // initialize a free list of extra buckets
    if (table->num_extra_buckets == 0)
        table->extra_bucket_free_list.head = 0;    // no extra bucket at all
    else
    {
        uint32_t extra_bucket_index;
        for (extra_bucket_index = 1; extra_bucket_index < 1 + table->num_extra_buckets - 1; extra_bucket_index++)
            (table->extra_buckets + extra_bucket_index)->next_extra_bucket_index = extra_bucket_index + 1;
        (table->extra_buckets + extra_bucket_index)->next_extra_bucket_index = 0;    // no more free extra bucket

        table->extra_bucket_free_list.head = 1;
    }



#ifdef MEHCACHED_COLLECT_STATS
    table->stats.count = 0;
#endif
    mehcached_reset_table_stats(table);
}


void
mehcached_table_init(struct mehcached_table *table, size_t num_buckets, bool concurrent_table_read, bool concurrent_table_write,const char * filename)
{
    assert((MEHCACHED_ITEMS_PER_BUCKET == 7 && sizeof(struct mehcached_bucket) == 64) || (MEHCACHED_ITEMS_PER_BUCKET == 15 && sizeof(struct mehcached_bucket) == 128) || (MEHCACHED_ITEMS_PER_BUCKET == 31 && sizeof(struct mehcached_bucket) == 256));

    assert(num_buckets > 0);

    size_t log_num_buckets = 0;
    while (((size_t)1 << log_num_buckets) < num_buckets)
        log_num_buckets++;
    num_buckets = (size_t)1 << log_num_buckets;
    assert(log_num_buckets <= 32);

    table->num_buckets = (uint32_t)num_buckets;
    table->num_buckets_mask = (uint32_t)num_buckets - 1;
    table->num_extra_buckets = table->num_buckets / 10;    // 10% of normal buckets

    table->buckets=new mehcached_bucket[table->num_buckets + table->num_extra_buckets];

    table->extra_buckets = table->buckets + table->num_buckets - 1; // subtract by one to compensate 1-base indices
    // the rest extra_bucket information is initialized in mehcached_table_reset()

    // we have to zero out buckets here because mehcached_table_reset() tries to free non-zero entries in the buckets
    memset(table->buckets, 0, sizeof(struct mehcached_bucket) * (table->num_buckets + table->num_extra_buckets));

    if (!concurrent_table_read)
        table->concurrent_access_mode = 0;
    else if (concurrent_table_read && !concurrent_table_write)
        table->concurrent_access_mode = 1;
    else
        table->concurrent_access_mode = 2;

    //init fawn flash
    table->ds=new fawn::FawnDS_Flash(filename);
    mehcached_table_reset(table);

#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("!NDEBUG (low performance)\n");
#endif

#ifdef MEHCACHED_VERBOSE
    printf("MEHCACHED_VERBOSE (low performance)\n");
#endif

#ifdef MEHCACHED_COLLECT_STATS
    printf("MEHCACHED_COLLECT_STATS (low performance)\n");
#endif

#ifdef MEHCACHED_CONCURRENT
    printf("MEHCACHED_CONCURRENT (low performance)\n");
#endif

#ifdef MEHCACHED_USE_PH
    printf("MEHCACHED_USE_PH\n");
#endif

#ifdef MEHCACHED_NO_EVICTION
    printf("MEHCACHED_NO_EVICTION\n");
#endif

    printf("num_buckets = %u\n", table->num_buckets);
    printf("num_extra_buckets = %u\n", table->num_extra_buckets);

    printf("\n");
}


void
mehcached_table_free(struct mehcached_table *table)
{
    assert(table);

    mehcached_table_reset(table);

    delete table->buckets;
    delete table->ds;
}

