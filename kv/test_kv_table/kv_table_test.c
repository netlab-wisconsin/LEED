#include "../kv_table.h"

#include <stdio.h>
#include <string.h>


#define VALUE_SIZE 1000
uint64_t key = 1;
char value[VALUE_SIZE] = "hello world!";
size_t value_size;
enum {SET1,DELETE1,GET0,DONE} state=SET1;
char const  *op_str[]={"set0","set1","delete1","get0"};
struct mehcached_table table;
struct kv_log _log;
struct kv_storage storage;
static void test_cb(bool success, void *cb_arg) {
    //mehcached_print_bucket(table.buckets);
    if (!success){
        fprintf(stderr, "%s failed.\n",op_str[(int)state]);
        return;
    }
    printf("%s successfully.\n",op_str[(int)state]);
    switch (state)
    {
    case SET1:
        key |= 1UL << 63;
        mehcached_set(&table, key, (uint8_t *)&key, 8, (uint8_t *)value, VALUE_SIZE, test_cb, NULL);
        state=DELETE1;
        break;
    case DELETE1:
        mehcached_delete(&table, key, (uint8_t *)&key, 8, test_cb, NULL);
        state=GET0;
        break;
    case GET0:
        key&=~(1UL << 63);
        memset(value,0,64);
        mehcached_get(&table,key,(uint8_t *)&key, 8,(uint8_t *)value,&value_size,test_cb, NULL);
        state=DONE;
        break;
    case DONE:
        printf("Get %lu bytes: %s\n",value_size,value);
        mehcached_table_free(&table);
        kv_log_fini(&_log);
        kv_storage_stop(&storage);
    }    
}
static void storage_start(void *arg) {
    kv_log_init(&_log, &storage, 0, 0);
    uint32_t num_items = 50;
    num_items = num_items * MEHCACHED_ITEMS_PER_BUCKET / (MEHCACHED_ITEMS_PER_BUCKET - 3);
    mehcached_table_init(&table, &_log, (num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET, true, 20);
    mehcached_set(&table, key, (uint8_t *)&key, 8, (uint8_t *)value, VALUE_SIZE, test_cb, NULL);
}

int main(int argc, char **argv) {
    kv_storage_start(&storage, argv[1],storage_start,NULL);  
}