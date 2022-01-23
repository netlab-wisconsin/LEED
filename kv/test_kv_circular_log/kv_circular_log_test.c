#include "../kv_circular_log.h"

#include <assert.h>
#include <stdio.h>

#include "../kv_app.h"
#include "../kv_memory.h"
struct kv_storage storage;
struct kv_circular_log circular_log;
uint8_t *buf;
enum { WRITE1, READ0, DONE } state = WRITE1;
char const *op_str[] = {
    "WRITE0",
    "WRITE1",
    "READ0",
};
static void test_cb(bool success, void *cb_arg) {
    if (!success) {
        fprintf(stderr, "%s failed.\n", op_str[(int)state]);
        kv_circular_log_fini(&circular_log);
        kv_storage_fini(&storage);
        kv_storage_free(buf);
        kv_app_stop(-1);
        return;
    }
    struct iovec iov[] = {{buf + storage.block_size, 1}, {buf + 2 * storage.block_size, 3}, {buf + 5 * storage.block_size, 5}};
    printf("%s successfully.\n", op_str[(int)state]);
    switch (state) {
        case WRITE1:
            kv_circular_log_appendv(&circular_log, iov, 3, test_cb, NULL);
            state = READ0;
            break;
        case READ0:
            kv_memset(buf, 0, 10 * storage.block_size);
            kv_circular_log_read(&circular_log, 16, buf, 10, test_cb, NULL);
            state = DONE;
            break;
        case DONE:
            for (uint32_t i = 0; i < 10; i++) puts(buf + i * storage.block_size);
            kv_circular_log_fini(&circular_log);
            kv_storage_fini(&storage);
            kv_storage_free(buf);
            kv_app_stop(0);
    }
}
static void start(void *arg) {
    kv_storage_init(&storage, 0);
    kv_circular_log_init(&circular_log, &storage, 1000, 20, 16, 16, 0, 0);
    buf = kv_storage_blk_alloc(&storage, 10);
    for (uint32_t i = 0; i < 10; i++) sprintf(buf + i * storage.block_size, "%u. hello", i);
    kv_circular_log_append(&circular_log, buf, 1, test_cb, NULL);
}

int main(int argc, char **argv) { kv_app_start_single_task(argv[1], start, NULL); }