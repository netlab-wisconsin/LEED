#include "../kv_table.h"

int main(int argc, char **argv) {
    struct storage data;
    storage_init(&data, argv[1]);
    struct kv_log log;
    kv_log_init(&log, &data, 0, 0);
    struct mehcached_table table;
    uint32_t num_items = 2048, valuesize = 1024;
    num_items = num_items * MEHCACHED_ITEMS_PER_BUCKET / (MEHCACHED_ITEMS_PER_BUCKET - 3);
    mehcached_table_init(&table, &log, (num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET, true, 20);

    mehcached_table_free(&table);
    kv_log_fini(&log);
    storage_fini(&data);
}