#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "../kv_table.h"
#include "../kv_app.h"
#include "../utils/city.h"
#include "../utils/timing.h"
#define ALIGN(a, b) (((a) + (b)-1) / (b) * (b))
struct {
    uint64_t num_items, read_num_items;
    uint32_t value_size;
    uint8_t extra_buckets_percentage;
    uint32_t concurrent_io_num;
    char json_config_file[1024];

} opt = {.num_items = 1024,
         .read_num_items = 512,
         .value_size = 1024,
         .extra_buckets_percentage = 20,
         .concurrent_io_num = 32,
         .json_config_file = "config.json"};
static void help(void) {
    // TODO: HELP TEXT
    printf("Some helpful text.\n");
    return;
}
static void get_options(int argc, char** argv) {
    int ch;
    while ((ch = getopt(argc, argv, "hn:r:s:b:c:i:")) != -1) switch (ch) {
            case 'h':
                help();
                break;
            case 'n':
                opt.num_items = atoll(optarg);
                break;
            case 'r':
                opt.read_num_items = atoll(optarg);
                break;
            case 's':
                opt.value_size = atol(optarg);
                break;
            case 'b':
                opt.extra_buckets_percentage = atoi(optarg);
                break;
            case 'c':
                strcpy(opt.json_config_file, optarg);
                break;
            case 'i':
                opt.concurrent_io_num = atol(optarg);
                break;
            default:
                help();
                exit(-1);
        }
}

static inline uint64_t index_to_key(uint64_t index) { return CityHash64((char*)&index, sizeof(uint64_t)); }
struct io_buffer_t {
    union {
        uint64_t hash;
        uint8_t buf[8];
    } key;
    uint8_t* value;
    size_t value_length;
};

struct io_buffer_t* io_buffer;
uint64_t* keys = NULL;
static struct mehcached_table table;
static struct kv_storage storage;
static struct kv_log _log;
static struct timeval tv_start, tv_end;
uint64_t total_io;
uint32_t concurrent_io = 0;

static void storage_stop(void) {
    if (keys) free(keys);
    for (size_t i = 0; i < opt.concurrent_io_num; i++) kv_storage_free(io_buffer[i].value);
    free(io_buffer);
    mehcached_table_free(&table);
    kv_storage_fini(&storage);
    kv_log_fini(&_log);
    kv_app_stop(0);
}
static void get_test(bool success, void* arg) {
    struct io_buffer_t* io = arg;
    if (!success) fprintf(stderr, "get fail. key hash: %lu\n", io->key.hash);
    if (!total_io) {
        if (--concurrent_io == 0) {
            gettimeofday(&tv_end, NULL);
            printf("Query rate: %f\n", ((double)opt.read_num_items / timeval_diff(&tv_start, &tv_end)));
            storage_stop();
        }
        return;
    }
    io->key.hash = keys[--total_io];
    mehcached_get(&table, io->key.hash, io->key.buf, 8, io->value, &io->value_length, get_test, arg);
}

static void start_get(void* arg) {
    // mehcached_print_buckets(&table);
    keys = calloc(opt.read_num_items, sizeof(uint64_t));
    for (uint64_t i = 0; i < opt.read_num_items; ++i) keys[i] = index_to_key(random() % opt.num_items);
    puts("keys generated.");
    total_io = opt.read_num_items;
    gettimeofday(&tv_start, NULL);
    for (concurrent_io = 0; concurrent_io != opt.concurrent_io_num; ++concurrent_io) get_test(true, io_buffer + concurrent_io);
}

static void fill_db(bool success, void* arg) {
    struct io_buffer_t* io = arg;
    if (!success) fprintf(stderr, "set fail:%s\n", io->value);
    if (!total_io) {
        if (--concurrent_io == 0) {
            puts("db created successfully.");
            start_get(NULL);
        }
        return;
    }
    io->key.hash = index_to_key(--total_io);
    sprintf(io->value, "%lu", total_io);
    mehcached_set(&table, io->key.hash, io->key.buf, 8, io->value, opt.value_size, fill_db, arg);
}

static void storage_start(void* arg) {
    kv_storage_init(&storage, 0);
    io_buffer = calloc(opt.concurrent_io_num, sizeof(struct io_buffer_t));
    for (size_t i = 0; i < opt.concurrent_io_num; i++)
        io_buffer[i].value = kv_storage_malloc(&storage, ALIGN(opt.value_size, storage.block_size));
    kv_log_init(&_log, &storage, 0, 0);
    uint32_t num_items = opt.num_items * MEHCACHED_ITEMS_PER_BUCKET / (MEHCACHED_ITEMS_PER_BUCKET - 3);
    uint32_t num_buckets = (num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET;
    mehcached_table_init(&table, &_log, num_buckets, true, opt.extra_buckets_percentage);
    total_io = opt.num_items;
    for (concurrent_io = 0; concurrent_io != 1 /*opt.concurrent_io_num*/; ++concurrent_io)
        fill_db(true, io_buffer + concurrent_io);
    // TODO: need to change kv_table_concurrent.c to support concurrent set.
}

int main(int argc, char** argv) {
    get_options(argc, argv);
    kv_app_start_single_task(opt.json_config_file, storage_start, NULL);
}