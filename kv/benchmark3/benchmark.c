#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "../benchmark2/city.h"
#include "../benchmark2/timing.h"
#include "../kv_app.h"
#include "../kv_data_store.h"
struct {
    uint64_t num_items, read_num_items;
    uint32_t value_size;
    uint32_t ssd_num;
    uint32_t concurrent_io_num;
    char json_config_file[1024];

} opt = {.num_items = 1024,
         .read_num_items = 512,
         .ssd_num = 2,
         .value_size = 1024,
         .concurrent_io_num = 32,
         .json_config_file = "config.json"};
static void help(void) {
    // TODO: HELP TEXT
    printf("Some helpful text.\n");
    return;
}
static void get_options(int argc, char **argv) {
    int ch;
    while ((ch = getopt(argc, argv, "hn:r:v:d:c:i:")) != -1) switch (ch) {
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
            case 'd':
                opt.ssd_num = atol(optarg);
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
static inline uint128 index_to_key(uint64_t index) { return CityHash128((char *)&index, sizeof(uint64_t)); }
struct worker {
    struct kv_storage storage;
    struct kv_data_store data_store;
};
struct io_buffer_t {
    union {
        uint128 hash;
        uint8_t buf[16];
    } key;
    uint8_t *value;
    uint32_t value_length;
    uint32_t worker_id;
    uint64_t index;
};
struct io_buffer_t *io_buffer;

struct worker *workers;
uint64_t total_io = 0, iocnt = 0;
static struct timeval tv_start, tv_end;
enum { INIT, FILL, READ, CLEAR } state = INIT;
char const *op_str[] = {"INIT", "FILL", "READ", "CLEAR"};

static void worker_stop(void *arg) {
    struct worker *self = arg;
    kv_data_store_fini(&self->data_store);
    kv_storage_fini(&self->storage);
    kv_app_stop(0);
}

static void stop(void) {
    for (size_t i = 0; i < opt.concurrent_io_num; i++) kv_storage_free(io_buffer[i].value);
    free(io_buffer);
    for (size_t i = 0; i < opt.ssd_num; i++) kv_app_send_msg(kv_app()->threads[i], worker_stop, workers + i);
    kv_app_stop(0);
}

static void test(void *arg);
static void io_fini(bool success, void *arg) {
    struct io_buffer_t *io = arg;
    if (!success) {
        fprintf(stderr, "%s fail. key index: %lu\n", op_str[(int)state], io->index);
    }
    kv_app_send_msg(kv_app()->threads[opt.ssd_num], test, arg);
}

static void io_start(void *arg) {
    struct io_buffer_t *io = arg;
    struct worker *self = workers + io->worker_id;
    switch (state) {
        case FILL:
            kv_data_store_set(&self->data_store, io->key.buf, 16, io->value, io->value_length, io_fini, arg);
            break;
        case READ:
            kv_data_store_get(&self->data_store, io->key.buf, 16, io->value, &io->value_length, io_fini, arg);
            break;
        case CLEAR:
            kv_data_store_delete(&self->data_store, io->key.buf, 16, io_fini, arg);
            break;
        case INIT:
            assert(false);
    }
}
static void test_fini(void) {
    gettimeofday(&tv_end, NULL);
    switch (state) {
        case INIT:
            printf("database initialized in %lf s.\n", timeval_diff(&tv_start, &tv_end));
            io_buffer = calloc(opt.concurrent_io_num, sizeof(struct io_buffer_t));
            for (size_t i = 0; i < opt.concurrent_io_num; i++)
                io_buffer[i].value = kv_storage_malloc(&workers[0].storage, opt.value_size + workers[0].storage.block_size);
            state = FILL;
            total_io = opt.num_items;
            break;
        case FILL:
            printf("Write rate: %f\n", ((double)opt.num_items / timeval_diff(&tv_start, &tv_end)));
            puts("db created successfully.");
            state = READ;
            total_io = opt.read_num_items;
            break;
        case READ:
            printf("Query rate: %f\n", ((double)opt.read_num_items / timeval_diff(&tv_start, &tv_end)));
            state = CLEAR;
            total_io = opt.num_items;
            break;
        case CLEAR:
            printf("Clear rate: %f\n", ((double)opt.num_items / timeval_diff(&tv_start, &tv_end)));
            stop();
            return;
    }
    gettimeofday(&tv_start, NULL);
    for (iocnt = 0; iocnt != opt.concurrent_io_num; ++iocnt) test(io_buffer + iocnt);
}

static void test(void *arg) {
    struct io_buffer_t *io = arg;
    if (total_io == 0) {
        if (--iocnt == 0) {
            test_fini();
        }
        return;
    }
    switch (state) {
        case FILL:
            io->index = --total_io;
            io->value_length = opt.value_size;
            sprintf(io->value, "%lu", io->index);
            memcpy(io->value + 20, io->key.buf, 16);
            break;
        case READ:
            --total_io;
            io->index = random() % opt.num_items;
            break;
        case CLEAR:
            io->index = --total_io;
            break;
        case INIT:
            assert(false);
    }
    io->key.hash = index_to_key(io->index);
    io->worker_id = io->key.hash.second % opt.ssd_num;
    kv_app_send_msg(kv_app()->threads[io->worker_id], io_start, arg);
}

static void send_init_done_msg(bool success, void *arg) {
    if (!success) {
        fprintf(stderr, "init fail!\n");
        exit(-1);
    }
    kv_app_send_msg(kv_app()->threads[opt.ssd_num], test, arg);
}

static void worker_init(void *arg) {
    struct worker *self = arg;
    kv_storage_init(&self->storage, self - workers);
    uint32_t bucket_num = opt.num_items / KV_ITEM_PER_BUCKET;
    uint64_t value_log_block_num = opt.value_size * opt.num_items * 1.05 / self->storage.block_size;
    kv_data_store_init(&self->data_store, &self->storage, 0, bucket_num, value_log_block_num, 512, send_init_done_msg, NULL);
}

int main(int argc, char **argv) {
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("DEBUG (low performance)\n");
#endif
    get_options(argc, argv);
    struct kv_app_task *task = calloc(opt.ssd_num + 1, sizeof(struct kv_app_task));
    workers = calloc(opt.ssd_num, sizeof(struct worker));
    for (size_t i = 0; i < opt.ssd_num; i++) {
        task[i].func = worker_init;
        task[i].arg = workers + i;
    }
    task[opt.ssd_num] = (struct kv_app_task){NULL, NULL};
    iocnt = opt.ssd_num;
    gettimeofday(&tv_start, NULL);
    kv_app_start(opt.json_config_file, opt.ssd_num + 1, task);
    free(workers);
    free(task);
    return 0;
}