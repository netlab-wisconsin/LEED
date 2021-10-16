#include "../kv_log.h"

#include <string.h>

#include "stdio.h"
#define CONCURRENT_IO_NUM 128
#define VALUE_SIZE 1000
uint64_t keys[CONCURRENT_IO_NUM];
uint8_t values[CONCURRENT_IO_NUM][1024];
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
    if (--io_num == 0) kv_storage_stop(log.storage);
}

static void write_complete_cb(bool success, void *cb_arg) {
    uint64_t i = *(uint64_t *)cb_arg;
    if (!success) {
        fprintf(stderr, "Write failed.\n");
        if (--io_num == 0) kv_storage_stop(log.storage);
        return;
    }
    printf("Write %lu successfully.\n", i);
    memset(values[i], 0, 64);
    kv_log_read(&log, offsets[i], (uint8_t *)(keys + i), sizeof(uint64_t), values[i], value_sizes + i, read_complete_cb,
                cb_arg);
}

static void storage_start(void *arg) {
    struct kv_storage *storage = arg;
    kv_log_init(&log, storage, 0, 0);
    for (size_t i = 0; i < CONCURRENT_IO_NUM; i++) {
        offsets[i] = log.tail;
        kv_log_write(&log, log.tail, (uint8_t *)(keys + i), sizeof(uint64_t), values[i], VALUE_SIZE, write_complete_cb,
                     keys + i);
    }
}

int main(int argc, char **argv) {
    for (size_t i = 0; i < CONCURRENT_IO_NUM; i++) {
        keys[i] = i;
        sprintf((char *)values[i], "%lu. hello world!", i);
    }
    struct kv_storage storage;
    kv_storage_start(&storage, argv[1], storage_start, &storage);
    kv_log_fini(&log);
}
