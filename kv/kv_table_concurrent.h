
#ifndef _KV_TABLE_CONCURRENT_H_
#define _KV_TABLE_CONCURRENT_H_
#include "kv_table.h"
uint32_t mehcached_read_version_begin(const struct mehcached_table *table, const struct mehcached_bucket *bucket);
uint32_t mehcached_read_version_end(const struct mehcached_table *table, const struct mehcached_bucket *bucket);
void mehcached_lock_bucket(const struct mehcached_table *table, struct mehcached_bucket *bucket);
void mehcached_unlock_bucket(const struct mehcached_table *table, struct mehcached_bucket *bucket);
void mehcached_lock_extra_bucket_free_list(struct mehcached_table *table);
void mehcached_unlock_extra_bucket_free_list(struct mehcached_table *table);
#endif