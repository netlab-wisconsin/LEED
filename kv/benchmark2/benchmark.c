
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "../kv_app.h"
#include "../kv_data_store.h"
#include "city.h"
#include "timing.h"
#define ALIGN(a, b) (((a) + (b)-1) / (b))
struct {
    uint64_t num_items, read_num_items;
    uint32_t value_size;
    uint32_t compact_buf_len;
    uint32_t concurrent_io_num;
    char json_config_file[1024];

} opt = {.num_items = 1024,
         .read_num_items = 512,
         .compact_buf_len = 256,
         .value_size = 1024,
         .concurrent_io_num = 32,
         .json_config_file = "config.json"};
static void help(void) {
    // TODO: HELP TEXT
    printf("Some helpful text.\n");
    return;
}
static void get_options(int argc, char** argv) {
    int ch;
    while ((ch = getopt(argc, argv, "hn:r:v:b:c:i:")) != -1) switch (ch) {
            case 'h':
                help();
                break;
            case 'n':
                opt.num_items = atoll(optarg);
                break;
            case 'r':
                opt.read_num_items = atoll(optarg);
                break;
            case 'v':
                opt.value_size = atol(optarg);
                break;
            case 'b':
                opt.compact_buf_len = atol(optarg);
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
    uint32_t value_length;
};

struct io_buffer_t* io_buffer;
uint64_t* keys = NULL;
struct kv_storage storage;
struct kv_data_store data_store;
static struct timeval tv_start, tv_end;
uint64_t total_io;
uint32_t concurrent_io = 0;

static void stop(void) {
    if (keys) free(keys);
    for (size_t i = 0; i < opt.concurrent_io_num; i++) kv_storage_free(io_buffer[i].value);
    free(io_buffer);
    kv_data_store_fini(&data_store);
    kv_storage_fini(&storage);
    kv_app_stop(0);
}
static void get_test(bool success, void* arg) {
    struct io_buffer_t* io = arg;
    if (!success) fprintf(stderr, "get fail. key hash: %lu\n", io->key.hash);
    if (!total_io) {
        if (--concurrent_io == 0) {
            gettimeofday(&tv_end, NULL);
            printf("Query rate: %f\n", ((double)opt.read_num_items / timeval_diff(&tv_start, &tv_end)));
            stop();
        }
        return;
    }
    io->key.hash = keys[--total_io];
    kv_data_store_get(&data_store, io->key.buf, 8, io->value, &io->value_length, get_test, arg);
}

static void start_get(void* arg) {
    keys = calloc(opt.read_num_items, sizeof(uint64_t));
    for (uint64_t i = 0; i < opt.read_num_items; ++i) keys[i] = index_to_key(random() % opt.num_items);
    puts("keys generated.");
    total_io = opt.read_num_items;
    gettimeofday(&tv_start, NULL);
    for (concurrent_io = 0; concurrent_io != opt.concurrent_io_num; ++concurrent_io) {
        get_test(true, io_buffer + concurrent_io);
    }
}

static void fill_db(bool success, void* arg) {
    struct io_buffer_t* io = arg;
    if (!success) fprintf(stderr, "set fail:%s\n", io->value);
    if (!total_io) {
        if (--concurrent_io == 0) {
            gettimeofday(&tv_end, NULL);
            printf("Write rate: %f\n", ((double)opt.num_items / timeval_diff(&tv_start, &tv_end)));
            puts("db created successfully.");
            start_get(NULL);
        }
        return;
    }
    io->key.hash = index_to_key(--total_io);
    sprintf(io->value, "%lu", total_io);
    kv_data_store_set(&data_store, io->key.buf, 8, io->value, opt.value_size, fill_db, arg);
}

static void start(bool success, void* arg) {
    if (!success) {
        fprintf(stderr, "data store init fail.\n");
        kv_storage_fini(&storage);
        kv_app_stop(-1);
        return;
    }
    gettimeofday(&tv_end, NULL);
    printf("database initialized in %lf s.\n", timeval_diff(&tv_start, &tv_end));
    io_buffer = calloc(opt.concurrent_io_num, sizeof(struct io_buffer_t));
    for (size_t i = 0; i < opt.concurrent_io_num; i++)
        io_buffer[i].value = kv_storage_malloc(&storage, opt.value_size + storage.block_size);
    total_io = opt.num_items;
    gettimeofday(&tv_start, NULL);
    for (concurrent_io = 0; concurrent_io != opt.concurrent_io_num; ++concurrent_io) fill_db(true, io_buffer + concurrent_io);
}
static void init(void* arg) {
    kv_storage_init(&storage, 0);
    uint32_t bucket_num = opt.num_items / KV_ITEM_PER_BUCKET;
    uint64_t value_log_block_num = ALIGN(opt.value_size, storage.block_size) * opt.num_items + 10;
    gettimeofday(&tv_start, NULL);
    kv_data_store_init(&data_store, &storage, 0, bucket_num, value_log_block_num, opt.compact_buf_len, start, NULL);
}
int main(int argc, char** argv) {
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("!NDEBUG (low performance)\n");
#endif
    get_options(argc, argv);
    kv_app_start_single_task(opt.json_config_file, init, NULL);
}