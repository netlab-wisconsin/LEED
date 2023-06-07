#include "../../kv_value_log.h"

#include <assert.h>
#include <stdio.h>

#include "../../kv_app.h"
struct kv_storage storage;
struct kv_value_log value_log;
uint8_t *buf;
enum { WRITE1, READ0, READ1, DONE } state = WRITE1;
char const *op_str[] = {"WRITE0", "WRITE1", "READ0", "READ1"};
static void test_cb(bool success, void *cb_arg) {
    // mehcached_print_bucket(table.buckets);
    if (!success) {
        fprintf(stderr, "%s failed.\n", op_str[(int)state]);
        kv_app_stop(-1);
        return;
    }
    static uint64_t offset = 0;
    printf("%s successfully.\n", op_str[(int)state]);
    switch (state) {
        case WRITE1:
            offset = kv_value_log_offset(&value_log);
            kv_value_log_write(&value_log, 1, buf + 3 * storage.block_size / 2, 5 * storage.block_size, test_cb, NULL);
            state = READ0;
            break;
        case READ0:
            kv_value_log_read(&value_log, offset, buf, 100, test_cb, NULL);
            state = READ1;
            break;
        case READ1:
            puts(buf);  //     3. hello
            kv_value_log_read(&value_log, 260, buf, 5 * storage.block_size, test_cb, NULL);
            state = DONE;
            break;
        case DONE:
            puts(buf);                               // 1. hello
            puts(buf + storage.block_size / 2);      // 2. hello
            puts(buf + 3 * storage.block_size / 2);  // 3. hello
            kv_value_log_fini(&value_log);
            kv_storage_fini(&storage);
            kv_storage_free(buf);
            kv_app_stop(0);
    }
}
static void start(void *arg) {
    kv_storage_init(&storage, 0);
    kv_value_log_init(&value_log, &storage, NULL,  storage.num_blocks / 3,
                       storage.num_blocks / 3, 1);
    buf = kv_storage_blk_alloc(&storage, 10);
    for (uint32_t i = 0; i < 20; i++) sprintf(buf + i * storage.block_size / 2, "    %u. hello", i);

    kv_value_log_write(&value_log, 0, buf, 1.5 * storage.block_size, test_cb, NULL);
}

int main(int argc, char **argv) { kv_app_start_single_task(argv[1], start, NULL); }