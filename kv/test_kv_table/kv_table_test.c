#include "../kv_table.h"

#include <stdio.h>

#include "pthread.h"

#define value_size 1000
uint64_t key = 1;
char value[value_size] = "hello world!";
static pthread_mutex_t g_io_mtx = PTHREAD_MUTEX_INITIALIZER;
static void set_test_cb(bool success, void *cb_arg) {
    if (!success)
        fprintf(stderr, "Write failed.\n");
    else
        printf("Write successfully.\n");
    pthread_mutex_unlock(&g_io_mtx);
}

static void set_test(struct mehcached_table *table) {
    pthread_mutex_lock(&g_io_mtx);
    mehcached_set(table, key, (uint8_t *)&key, 8, (uint8_t *)value, value_size, set_test_cb, NULL);
    pthread_mutex_lock(&g_io_mtx);
    mehcached_print_bucket(table->buckets);
    key |= 1UL << 63;
    mehcached_set(table, key, (uint8_t *)&key, 8, (uint8_t *)value, value_size, set_test_cb, NULL);
    pthread_mutex_lock(&g_io_mtx);
    mehcached_print_bucket(table->buckets);
}

int main(int argc, char **argv) {
    struct storage data;
    storage_init(&data, argv[1]);
    struct kv_log log;
    kv_log_init(&log, &data, 0, 0);
    struct mehcached_table table;
    uint32_t num_items = 50;
    num_items = num_items * MEHCACHED_ITEMS_PER_BUCKET / (MEHCACHED_ITEMS_PER_BUCKET - 3);
    mehcached_table_init(&table, &log, (num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET, true, 20);
    set_test(&table);
    mehcached_table_free(&table);
    kv_log_fini(&log);
    storage_fini(&data);
}