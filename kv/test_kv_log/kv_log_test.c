#include "../kv_log.h"

#include <string.h>

#include "../kv_app.h"
#include "stdio.h"
#define CONCURRENT_IO_NUM 1000
#define VALUE_SIZE 1000
uint64_t keys[CONCURRENT_IO_NUM];
uint8_t *values[CONCURRENT_IO_NUM];
uint32_t value_sizes[CONCURRENT_IO_NUM];
uint64_t offsets[CONCURRENT_IO_NUM];
static struct kv_log log;
uint32_t io_num = CONCURRENT_IO_NUM;

static void read_complete_cb(bool success, void *cb_arg) {
    uint64_t i = *(uint64_t *)cb_arg;
    if (!success)
        fprintf(stderr, "Read %lu failed.\n", i);
    else
        printf("read %u bytes: %s\n", value_sizes[i], values[i]);
    if (--io_num == 0) {
        kv_storage_fini(log.storage);
        kv_log_fini(&log);
        for (size_t i = 0; i < CONCURRENT_IO_NUM; i++) kv_storage_free(values[i]);
        kv_app_stop(0);
    }
}

static void write_complete_cb(bool success, void *cb_arg) {
    uint64_t i = *(uint64_t *)cb_arg;
    if (!success) {
        fprintf(stderr, "Write failed.\n");
        if (--io_num == 0) {
            kv_storage_fini(log.storage);
            kv_log_fini(&log);
            for (size_t i = 0; i < CONCURRENT_IO_NUM; i++) kv_storage_free(values[i]);
            kv_app_stop(-1);
        }
        return;
    }
    printf("Write %lu successfully.\n", i);
    memset(values[i], 0, 64);
    kv_log_read(&log, offsets[i], (uint8_t *)(keys + i), sizeof(uint64_t), values[i], value_sizes + i, read_complete_cb,
                cb_arg);
}

static void start(void *arg) {
    struct kv_storage *storage = arg;
    kv_storage_init(storage, 0);
    for (size_t i = 0; i < CONCURRENT_IO_NUM; i++) {
        keys[i] = i;
        values[i] = kv_storage_malloc(storage, 1024);
        sprintf((char *)values[i], "%lu. hello world!", i);
    }
    kv_log_init(&log, storage, 0, 0);
    for (size_t i = 0; i < CONCURRENT_IO_NUM; i++) {
        kv_log_write(&log, log.tail, (uint8_t *)(keys + i), sizeof(uint64_t), values[i], VALUE_SIZE, write_complete_cb,
                     keys + i);
        offsets[i] = kv_log_get_offset(&log);
    }
}

int main(int argc, char **argv) {
    struct kv_storage storage;
    kv_app_start_single_task(argv[1], start, &storage);
}
