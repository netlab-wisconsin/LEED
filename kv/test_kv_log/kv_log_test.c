#include "../kv_log.h"

#include "pthread.h"
#include "stdio.h"
#define CONCURRENT_IO_NUM 128
#define VALUE_SIZE 1000
uint64_t keys[CONCURRENT_IO_NUM];
uint8_t values[CONCURRENT_IO_NUM][1024];
uint32_t value_sizes[CONCURRENT_IO_NUM];
uint64_t offsets[CONCURRENT_IO_NUM];
static struct kv_log log;
static pthread_mutex_t g_io_mtx = PTHREAD_MUTEX_INITIALIZER;
uint32_t io_num = CONCURRENT_IO_NUM;

static void read_complete_cb(bool success, void *cb_arg) {
    uint64_t i = *(uint64_t *)cb_arg;
    if (!success)
        fprintf(stderr, "Read %lu failed.\n", i);
    else
        printf("read %u bytes: %s\n", value_sizes[i], values[i]);
    if (--io_num == 0)
        pthread_mutex_unlock(&g_io_mtx);
}

static void write_complete_cb(bool success, void *cb_arg) {
    uint64_t i = *(uint64_t *)cb_arg;
    if (!success) {
        fprintf(stderr, "Write failed.\n");
        if (--io_num == 0)
            pthread_mutex_unlock(&g_io_mtx);
        return;
    }
    printf("Write %lu successfully.\n", i);
    kv_log_read(&log, offsets[i], (uint8_t *)(keys + i), sizeof(uint64_t), values[i], value_sizes + i, read_complete_cb, cb_arg);
}

int main(int argc, char **argv) {
    for (size_t i = 0; i < CONCURRENT_IO_NUM; i++) {
        keys[i] = i;
        sprintf((char *)values[i], "%lu. hello world!", i);
    }
    struct storage data;
    storage_init(&data, argv[1]);
    kv_log_init(&log, &data, 0, 0);
    pthread_mutex_lock(&g_io_mtx);
    for (size_t i = 0; i < CONCURRENT_IO_NUM; i++) {
        offsets[i] = log.tail;
        kv_log_write(&log, log.tail, (uint8_t *)(keys + i), sizeof(uint64_t), values[i], VALUE_SIZE, write_complete_cb, keys + i);
    }
    pthread_mutex_lock(&g_io_mtx);
    kv_log_fini(&log);
    storage_fini(&data);
}
