#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "../kv_app.h"
#include "../kv_data_store.h"
#include "../utils/city.h"
#include "../utils/timing.h"
struct {
    uint64_t num_items, read_num_items;
    uint32_t value_size;
    uint32_t ssd_num;
    uint32_t producer_num;
    uint32_t concurrent_io_num;
    char json_config_file[1024];

} opt = {.num_items = 1024,
         .read_num_items = 512,
         .ssd_num = 2,
         .producer_num = 1,
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
    while ((ch = getopt(argc, argv, "hn:r:v:d:c:i:p:")) != -1) switch (ch) {
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
            case 'p':
                opt.producer_num = atol(optarg);
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
} * workers;

struct producer {
    uint64_t start_io, end_io;
    uint64_t iocnt;
} * producers;
struct io_buffer_t {
    union {
        uint128 hash;
        uint8_t buf[16];
    } key;
    uint8_t *value;
    uint32_t value_length;
    uint32_t worker_id;
    uint32_t producer_id;
    uint64_t index;
    kv_data_store_ctx ctx;
};
struct io_buffer_t *io_buffer;
struct kv_ds_queue ds_queue;

static struct timeval tv_start, tv_end;
enum { INIT,
       FILL,
       READ,
       CLEAR } state = INIT;
char const *op_str[] = {"INIT", "FILL", "READ", "CLEAR"};

static void worker_stop(void *arg) {
    struct worker *self = arg;
    kv_data_store_fini(&self->data_store);
    kv_storage_fini(&self->storage);
    kv_app_stop(0);
}
static void producer_stop(void *arg) { kv_app_stop(0); }

static void stop(void) {
    for (size_t i = 0; i < opt.concurrent_io_num; i++) kv_storage_free(io_buffer[i].value);
    free(io_buffer);
    for (size_t i = 0; i < opt.ssd_num; i++) kv_app_send(i, worker_stop, workers + i);
    for (size_t i = 0; i < opt.producer_num; i++) kv_app_send(opt.ssd_num + i, producer_stop, NULL);
}

static void test(void *arg);
static void io_fini(bool success, void *arg) {
    struct io_buffer_t *io = arg;
    if (!success) {
        fprintf(stderr, "%s fail. key index: %lu\n", op_str[(int)state], io->index);
        exit(-1);
    }
    if (state == FILL) {
        kv_data_store_set_commit(io->ctx, true);
    } else if (state == CLEAR) {
        kv_data_store_del_commit(io->ctx, true);
    }
    kv_app_send(opt.ssd_num + io->producer_id, test, arg);
}

static void io_start(void *arg) {
    struct io_buffer_t *io = arg;
    struct worker *self = workers + io->worker_id;
    switch (state) {
        case FILL:
            io->ctx = kv_data_store_set(&self->data_store, io->key.buf, 16, io->value, io->value_length, io_fini, arg);
            break;
        case READ:
            kv_data_store_get(&self->data_store, io->key.buf, 16, io->value, &io->value_length, io_fini, arg);
            break;
        case CLEAR:
            io->ctx = kv_data_store_delete(&self->data_store, io->key.buf, 16, io_fini, arg);
            break;
        case INIT:
            assert(false);
    }
}
static void test_fini(void *arg) {  // always running on producer 0
    uint64_t total_io = 0;
    static uint32_t producer_cnt = 1;
    if (--producer_cnt) return;
    gettimeofday(&tv_end, NULL);
    producer_cnt = opt.producer_num;
    switch (state) {
        case INIT:
            printf("database initialized in %lf s.\n", timeval_diff(&tv_start, &tv_end));
            io_buffer = calloc(opt.concurrent_io_num, sizeof(struct io_buffer_t));
            for (size_t i = 0; i < opt.concurrent_io_num; i++)
                io_buffer[i].value = kv_storage_malloc(&workers[0].storage, opt.value_size + workers[0].storage.block_size);
            state = FILL;
            total_io = opt.num_items * opt.ssd_num;
            break;
        case FILL:
            printf("Write rate: %f\n", ((double)opt.num_items * opt.ssd_num / timeval_diff(&tv_start, &tv_end)));
            puts("db created successfully.");
            state = READ;
            total_io = opt.read_num_items * opt.ssd_num;
            break;
        case READ:
            printf("Query rate: %f\n", ((double)opt.read_num_items * opt.ssd_num / timeval_diff(&tv_start, &tv_end)));
            state = CLEAR;
            total_io = opt.num_items * opt.ssd_num;
            break;
        case CLEAR:
            printf("Clear rate: %f\n", ((double)opt.num_items * opt.ssd_num / timeval_diff(&tv_start, &tv_end)));
            stop();
            return;
    }
    gettimeofday(&tv_start, NULL);
    uint64_t io_per_producer = total_io / opt.producer_num;
    for (size_t i = 0; i < opt.producer_num; i++) {
        producers[i].iocnt = opt.concurrent_io_num / opt.producer_num;
        producers[i].start_io = i * io_per_producer;
        producers[i].end_io = (i + 1) * io_per_producer;
        for (size_t j = i * producers[i].iocnt; j < (i + 1) * producers[i].iocnt; j++) {
            io_buffer[j].producer_id = i;
            kv_app_send(opt.ssd_num + i, test, io_buffer + j);
        }
    }
}

static void test(void *arg) {
    struct io_buffer_t *io = arg;
    struct producer *p = io ? producers + io->producer_id : producers;

    if (p->start_io == p->end_io) {
        if (--p->iocnt == 0) {
            kv_app_send(opt.ssd_num, test_fini, NULL);
        }
        return;
    }
    switch (state) {
        case FILL:
            io->index = p->start_io;
            io->value_length = opt.value_size;
            sprintf(io->value, "%lu", io->index);
            memcpy(io->value + 20, io->key.buf, 16);
            break;
        case READ:
            io->index = random() % (opt.num_items * opt.ssd_num);
            break;
        case CLEAR:
            io->index = p->start_io;
            break;
        case INIT:
            assert(false);
    }
    p->start_io++;
    io->key.hash = index_to_key(io->index);
    io->worker_id = io->key.hash.second % opt.ssd_num;
    kv_app_send(io->worker_id, io_start, arg);
}

static void send_init_done_msg(bool success, void *arg) {
    if (!success) {
        fprintf(stderr, "init fail!\n");
        exit(-1);
    }
    kv_app_send(opt.ssd_num, test, arg);
}

static void worker_init(void *arg) {
    struct worker *self = arg;
    kv_storage_init(&self->storage, self - workers);
    uint32_t bucket_num = opt.num_items / KV_ITEM_PER_BUCKET;
    uint64_t value_log_block_num = opt.value_size * opt.num_items * 1.4 / self->storage.block_size;
    kv_data_store_init(&self->data_store, &self->storage, 0, bucket_num, value_log_block_num, 512, &ds_queue, self - workers,
                       send_init_done_msg, NULL);
}

int main(int argc, char **argv) {
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("DEBUG (low performance)\n");
#endif
    get_options(argc, argv);
    struct kv_app_task *task = calloc(opt.ssd_num + opt.producer_num, sizeof(struct kv_app_task));
    workers = calloc(opt.ssd_num, sizeof(struct worker));
    for (size_t i = 0; i < opt.ssd_num; i++) {
        task[i].func = worker_init;
        task[i].arg = workers + i;
    }
    producers = calloc(opt.producer_num, sizeof(struct producer));
    for (size_t i = 0; i < opt.producer_num; i++) {
        task[opt.ssd_num + i] = (struct kv_app_task){NULL, NULL};
    }
    *producers = (struct producer){0, 0, opt.ssd_num};
    kv_ds_queue_init(&ds_queue, opt.ssd_num);
    gettimeofday(&tv_start, NULL);
    kv_app_start(opt.json_config_file, opt.ssd_num + opt.producer_num, task);
    kv_ds_queue_fini(&ds_queue);
    free(producers);
    free(workers);
    free(task);
    return 0;
}