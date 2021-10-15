#include "kv_table_concurrent.h"

#include <assert.h>
#ifdef MEHCACHED_CONCURRENT
#define memory_barrier()               \
    do {                               \
        asm volatile("" ::: "memory"); \
    } while (0)
#endif

// uint64_t
uint32_t mehcached_read_version_begin(const struct mehcached_table *table, const struct mehcached_bucket *bucket) {
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode != 0) {
        while (true) {
            // uint64_t v = *(volatile uint64_t *)&bucket->version;
            uint32_t v = *(volatile uint32_t *)&bucket->version;
            memory_barrier();
            // if ((v & 1UL) != 0UL)
            if ((v & 1U) != 0U) continue;
            return v;
        }
    } else
        // return 0UL;
        return 0U;
#else
    // return 0UL;
    return 0U;
#endif
}

// uint64_t
uint32_t mehcached_read_version_end(const struct mehcached_table *table, const struct mehcached_bucket *bucket) {
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode != 0) {
        memory_barrier();
        // uint64_t v = *(volatile uint64_t *)&bucket->version;
        uint32_t v = *(volatile uint32_t *)&bucket->version;
        return v;
    } else
        // return 0UL;
        return 0U;
#else
    // return 0UL;
    return 0U;
#endif
}

void mehcached_lock_bucket(const struct mehcached_table *table, struct mehcached_bucket *bucket) {
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode == 1) {
        // assert((*(volatile uint64_t *)&bucket->version & 1UL) == 0UL);
        // (*(volatile uint64_t *)&bucket->version)++;
        assert((*(volatile uint32_t *)&bucket->version & 1U) == 0U);
        (*(volatile uint32_t *)&bucket->version)++;
        memory_barrier();
    } else if (table->concurrent_access_mode == 2) {
        while (1) {
            // uint64_t v = *(volatile uint64_t *)&bucket->version & ~1UL;
            uint32_t v = *(volatile uint32_t *)&bucket->version & ~1U;
            // uint64_t new_v = v | 1UL;
            uint32_t new_v = v | 1U;
            // if (__sync_bool_compare_and_swap((volatile uint64_t *)&bucket->version, v, new_v))
            if (__sync_bool_compare_and_swap((volatile uint32_t *)&bucket->version, v, new_v)) break;
        }
    }
#endif
}

void mehcached_unlock_bucket(const struct mehcached_table *table, struct mehcached_bucket *bucket) {
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode != 0) {
        memory_barrier();
        // assert((*(volatile uint64_t *)&bucket->version & 1UL) == 1UL);
        assert((*(volatile uint32_t *)&bucket->version & 1U) == 1U);
        // no need to use atomic add because this thread is the only one writing to version
        // (*(volatile uint64_t *)&bucket->version)++;
        (*(volatile uint32_t *)&bucket->version)++;
    }
#endif
}

void mehcached_lock_extra_bucket_free_list(struct mehcached_table *table) {
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode == 2) {
        while (1) {
            if (__sync_bool_compare_and_swap((volatile uint32_t *)&table->extra_bucket_free_list.lock, 0U, 1U)) break;
        }
    }
#endif
}

void mehcached_unlock_extra_bucket_free_list(struct mehcached_table *table) {
#ifdef MEHCACHED_CONCURRENT
    if (table->concurrent_access_mode == 2) {
        memory_barrier();
        assert((*(volatile uint32_t *)&table->extra_bucket_free_list.lock & 1U) == 1U);
        // no need to use atomic add because this thread is the only one writing to version
        *(volatile uint32_t *)&table->extra_bucket_free_list.lock = 0U;
    }
#endif
}