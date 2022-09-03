#include "../kv_data_store.h"

#include <stdio.h>

#include "../kv_app.h"
#include "../kv_memory.h"
#define VALUE_NUM 3
struct kv_storage storage;
struct kv_data_store data_store;
struct kv_ds_queue ds_queue;
uint8_t *key[] = {"00000000", "00000000", "00000000"};
uint8_t *value[VALUE_NUM];
kv_data_store_ctx ds_ctx[VALUE_NUM];
uint32_t value_length, io_cnt = VALUE_NUM;
enum { INIT,
       SET0,
       GET0,
       DELETE } state = INIT;
char const *op_str[] = {"INIT", "SET0", "GET0", "DELETE"};
static void test_cb(bool success, void *cb_arg) {
    if (!success) {
        fprintf(stderr, "%s failed.\n", op_str[(int)state]);
        kv_data_store_fini(&data_store);
        kv_storage_fini(&storage);
        for (size_t i = 0; i < VALUE_NUM; i++) kv_storage_free(value[i]);
        kv_app_stop(-1);
        return;
    }
    printf("%s successfully.\n", op_str[(int)state]);
    switch (state) {
        case INIT:
            state = SET0;
            sprintf(value[0], "hello world!");
            sprintf(value[1], "hi!");
            sprintf(value[2], "bye!");
            ds_ctx[0] = kv_data_store_set(&data_store, key[0], 8, value[0], 256, test_cb, ds_ctx);
            ds_ctx[1] = kv_data_store_set(&data_store, key[1], 8, value[1], 513, test_cb, ds_ctx + 1);
            ds_ctx[2] = kv_data_store_set(&data_store, key[2], 8, value[2], 1000, test_cb, ds_ctx + 2);
            break;
        case SET0:
            kv_data_store_set_commit(*(kv_data_store_ctx *)cb_arg, true);
            if (--io_cnt) return;
            state = GET0;
            kv_memset(value[0], 0, 5 * storage.block_size);
            kv_data_store_get(&data_store, key[0], 8, value[0], &value_length, test_cb, NULL);
            break;
        case GET0:
            puts(value[0]);
            printf("value length: %u\n", value_length);
            state = DELETE;
            ds_ctx[0] = kv_data_store_delete(&data_store, key[0], 8, test_cb, NULL);
            break;
        case DELETE:
            kv_data_store_del_commit(ds_ctx[0], true);
            kv_data_store_fini(&data_store);
            kv_storage_fini(&storage);
            for (size_t i = 0; i < VALUE_NUM; i++) kv_storage_free(value[i]);
            kv_app_stop(0);
    }
}

static void start(void *arg) {
    kv_storage_init(&storage, 0);
    for (size_t i = 0; i < VALUE_NUM; i++) value[i] = kv_storage_blk_alloc(&storage, 5);
    kv_data_store_init(&data_store, &storage, 0, 1 << 10, 14 << 10, 256, &ds_queue, 0);
    test_cb(true, NULL);
}

int main(int argc, char **argv) {
    kv_ds_queue_init(&ds_queue, 1);
    kv_app_start_single_task(argv[1], start, NULL);
    kv_ds_queue_fini(&ds_queue);
}