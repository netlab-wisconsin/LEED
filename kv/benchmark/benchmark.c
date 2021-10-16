#include <getopt.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../kv_table.h"
#include "city.h"

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
                opt.concurrent_io_num=atol(optarg);
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

struct io_buffer_t *io_buffer;
static struct mehcached_table table;
static struct kv_storage storage;
//static pthread_mutex_t g_io_mtx = PTHREAD_MUTEX_INITIALIZER;
uint64_t total_io;
uint32_t concurrent_io = 0;

static void fill_db(bool success, void* arg) {
    struct io_buffer_t* io = arg;
    if (!total_io) return;
    io->key.hash = index_to_key(--total_io);
    sprintf(io->value, "%lu", total_io);
    mehcached_set(&table, io->key.hash, io->key.buf, 8, io->value, opt.value_size, fill_db, arg);
}

static void storage_start(void *log) {
    kv_log_init(log, &storage, 0, 0);
    uint32_t num_items = opt.num_items * MEHCACHED_ITEMS_PER_BUCKET / (MEHCACHED_ITEMS_PER_BUCKET - 3);
    uint32_t num_buckets = (num_items + MEHCACHED_ITEMS_PER_BUCKET - 1) / MEHCACHED_ITEMS_PER_BUCKET;
    mehcached_table_init(&table, log, num_buckets, true, opt.extra_buckets_percentage);

}

int main(int argc, char** argv) {
    get_options(argc, argv);
    struct kv_log log;
    io_buffer=calloc(opt.concurrent_io_num,sizeof(struct io_buffer_t));
    for (size_t i = 0; i < opt.concurrent_io_num; i++) io_buffer->value = malloc(opt.value_size);
    total_io = opt.num_items;
    kv_storage_start(&storage, opt.json_config_file,storage_start,&log);



    

    for (size_t i = 0; i < opt.concurrent_io_num; i++) free(io_buffer->value);
    mehcached_table_free(&table);
    kv_log_fini(&log);
    kv_storage_stop(&storage);
}