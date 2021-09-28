#include "table.h"


void test(){
    mehcached_table table;
    uint32_t num_items=2048,valuesize=1024;
    num_items = num_items * MEHCACHED_ITEMS_PER_BUCKET / (MEHCACHED_ITEMS_PER_BUCKET -2);
    mehcached_table_init(&table,(num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET,
                         true, false,"/root/test_db");
    uint64_t key=0;
    string value(valuesize, 'a'),value2;
    mehcached_set(&table, key, (uint8_t *)&key, 8, reinterpret_cast<const uint8_t *>(value.c_str()), value.length());
    mehcached_get(&table,key,(uint8_t *)&key,8,value2);
    mehcached_delete(&table,key,(uint8_t *)&key,8);
    bool r=mehcached_get(&table,key,(uint8_t *)&key,8,value2);
    printf("%d",r);
    mehcached_table_free(&table);
}

int main(){
    test();

    return 0;
}