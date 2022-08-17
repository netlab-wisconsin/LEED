#include <assert.h>
#include <getopt.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "../kv_app.h"
#include "../kv_data_store.h"
#include "../kv_msg.h"
#include "../utils/city.h"
#include "../utils/timing.h"
#include "../ycsb/kv_ycsb.h"
struct {
    uint64_t num_items, operation_cnt;
    uint32_t value_size;
    uint32_t ssd_num;
    uint32_t producer_num;
    uint32_t concurrent_io_num;
    bool seq_read, fill, seq_write;
    char json_config_file[1024];
    char workload_file[1024];

} opt = {.num_items = 1024,
         .operation_cnt = 512,
         .ssd_num = 2,
         .producer_num = 1,
         .value_size = 1024,
         .concurrent_io_num = 32,
         .json_config_file = "config.json",
         .seq_read = false,
         .seq_write = false,
         .fill = false};
static void help(void) {
    // TODO: HELP TEXT
    printf("Some helpful text.\n");
    return;
}
static void get_options(int argc, char **argv) {
    int ch;
    while ((ch = getopt(argc, argv, "hd:w:c:i:P:RWF")) != -1) switch (ch) {
            case 'h':
                help();
                break;
            case 'w':
                strcpy(opt.workload_file, optarg);
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
            case 'P':
                opt.producer_num = atol(optarg);
                break;
            case 'R':
                opt.seq_read = true;
                break;
            case 'W':
                opt.seq_write = true;
                break;
            case 'F':
                opt.fill = true;
                break;
            default:
                help();
                exit(-1);
        }
}

struct worker {
    struct kv_storage storage;
    struct kv_data_store data_store;
} * workers;

struct producer {
    uint64_t start_io, end_io;
    uint64_t iocnt;
    double latency_sum;
    uint32_t io_per_record;
} * producers;
struct io_buffer_t {
    uint32_t worker_id;
    uint32_t producer_id;
    struct kv_msg *msg;
    bool read_modify_write, is_finished;
    struct timeval io_start;
};
struct io_buffer_t *io_buffers;
struct kv_ds_queue ds_queue;

#define LATENCY_MAX_RECORD 0x100000  // 1M
static double latency_records[LATENCY_MAX_RECORD];

static struct timeval tv_start, tv_end;
enum {
    INIT,
    FILL,
    TRANSACTION,
    SEQ_READ,
    SEQ_WRITE,
} state = INIT;

kv_ycsb_handle workload;

static void worker_stop(void *arg) {
    struct worker *self = arg;
    kv_data_store_fini(&self->data_store);
    kv_storage_fini(&self->storage);
    kv_app_stop(0);
}
static void producer_stop(void *arg) { kv_app_stop(0); }

static void stop(void) {
    for (size_t i = 0; i < opt.concurrent_io_num; i++) kv_storage_free(io_buffers[i].msg);
    free(io_buffers);
    for (size_t i = 0; i < opt.ssd_num; i++) kv_app_send(i, worker_stop, workers + i);
    for (size_t i = 0; i < opt.producer_num; i++) kv_app_send(opt.ssd_num + i, producer_stop, NULL);
}

static void test(void *arg);
static void io_fini(bool success, void *arg) {
    struct io_buffer_t *io = arg;
    io->msg->type = success ? KV_MSG_OK : KV_MSG_ERR;
    if (!success) {
        fprintf(stderr, "io fail. \n");
        exit(-1);
    }
    kv_app_send(opt.ssd_num + io->producer_id, test, arg);
}

static void io_start(void *arg) {
    struct io_buffer_t *io = arg;
    struct worker *self = workers + io->worker_id;
    struct kv_msg *msg = io->msg;
    switch (msg->type) {
        case KV_MSG_SET:
            kv_data_store_set(&self->data_store, KV_MSG_KEY(msg), msg->key_len, KV_MSG_VALUE(msg), msg->value_len, io_fini,
                              arg);
            break;
        case KV_MSG_GET:
            kv_data_store_get(&self->data_store, KV_MSG_KEY(msg), msg->key_len, KV_MSG_VALUE(msg), &msg->value_len, io_fini,
                              arg);
            break;
        case KV_MSG_DEL:
            kv_data_store_delete(&self->data_store, KV_MSG_KEY(msg), msg->key_len, io_fini, arg);
            break;
        case INIT:
            assert(false);
    }
}

static int double_cmp(const void *_a, const void *_b) {
    const double *a = _a, *b = _b;
    if (*a == *b) return 0;
    return *a < *b ? -1 : 1;
}
static void test(void *arg);
static void test_fini(void *arg) {  // always running on producer 0
    static uint64_t total_io = 0;
    static uint32_t io_per_record = 0;
    static uint32_t producer_cnt = 1;
    if (--producer_cnt) return;
    gettimeofday(&tv_end, NULL);
    producer_cnt = opt.producer_num;
    double latency_sum = 0;
    for (size_t i = 0; i < opt.producer_num; i++) {
        latency_sum += producers[i].latency_sum;
        producers[i].latency_sum = 0;
    }
    if (total_io) {
        qsort(latency_records, total_io / io_per_record, sizeof(double), double_cmp);
        printf("99.9%%  tail latency: %lf us\n", latency_records[(uint32_t)(total_io * 0.999 / io_per_record)] * 1000000);
        printf("average latency: %lf us\n", latency_sum * 1000000 / total_io);
    }
    switch (state) {
        case INIT:
            io_buffers = calloc(opt.concurrent_io_num, sizeof(struct io_buffer_t));
            for (size_t i = 0; i < opt.concurrent_io_num; i++)
                io_buffers[i].msg = kv_storage_malloc(&workers[0].storage,
                                                      opt.value_size + sizeof(struct kv_msg) + 16 + workers[0].storage.block_size);
            printf("rdma client initialized in %lf s.\n", timeval_diff(&tv_start, &tv_end));
            if (opt.fill) {
                total_io = opt.num_items;
                state = FILL;
            } else {
                state = opt.seq_read ? SEQ_READ : opt.seq_write ? SEQ_WRITE : TRANSACTION;
                total_io = opt.operation_cnt;
            }
            break;
        case FILL:
            printf("Write rate: %lf\n", ((double)opt.num_items / timeval_diff(&tv_start, &tv_end)));
            printf("db created successfully in %lf s.\n", timeval_diff(&tv_start, &tv_end));
            state = opt.seq_read ? SEQ_READ : opt.seq_write ? SEQ_WRITE : TRANSACTION;
            total_io = opt.operation_cnt;
            break;
        case SEQ_WRITE:
            printf("SEQ_WRITE rate: %lf\n", ((double)opt.operation_cnt / timeval_diff(&tv_start, &tv_end)));
            stop();
            return;
        case SEQ_READ:
            printf("SEQ_READ rate: %lf\n", ((double)opt.operation_cnt / timeval_diff(&tv_start, &tv_end)));
            stop();
            return;
        case TRANSACTION:
            printf("TRANSACTION rate: %lf\n", ((double)opt.operation_cnt / timeval_diff(&tv_start, &tv_end)));
            stop();
            return;
    }
    for (size_t i = 0; i < opt.concurrent_io_num; i++) io_buffers[i].is_finished = false;
    io_per_record = (uint32_t)ceil(((double)total_io) / LATENCY_MAX_RECORD);
    uint64_t io_per_producer = total_io / opt.producer_num;
    for (size_t i = 0; i < opt.producer_num; i++) {
        producers[i].io_per_record = io_per_record;
        producers[i].iocnt = opt.concurrent_io_num / opt.producer_num;
        producers[i].start_io = i * io_per_producer;
        producers[i].end_io = (i + 1) * io_per_producer;
        for (size_t j = i * producers[i].iocnt; j < (i + 1) * producers[i].iocnt; j++) {
            io_buffers[j].producer_id = i;
            kv_app_send(opt.ssd_num + i, test, io_buffers + j);
        }
    }
    gettimeofday(&tv_start, NULL);
}
static inline void do_transaction(struct io_buffer_t *io, struct kv_msg *msg) {
    msg->key_len = 16;
    enum kv_ycsb_operation op = kv_ycsb_next(workload, false, KV_MSG_KEY(msg), KV_MSG_VALUE(msg));
    switch (op) {
        case YCSB_READMODIFYWRITE:
            io->read_modify_write = true;
        // fall through
        case YCSB_READ:
            msg->type = KV_MSG_GET;
            msg->value_len = 0;
            break;
        case YCSB_UPDATE:
        case YCSB_INSERT:
            msg->type = KV_MSG_SET;
            msg->value_len = opt.value_size;
            break;
        default:
            assert(false);
    }
}
static void test(void *arg) {
    struct io_buffer_t *io = arg;
    struct producer *p = io ? producers + io->producer_id : producers;
    if (io && io->is_finished) {
        struct timeval io_end;
        gettimeofday(&io_end, NULL);
        double latency = timeval_diff(&io->io_start, &io_end);
        p->latency_sum += latency;
        if (p->start_io % p->io_per_record == 0) {
            latency_records[p->start_io / p->io_per_record] = latency;
        }
        if (io->msg->type != KV_MSG_OK) {
            fprintf(stderr, "io fail. \n");
            exit(-1);
        } else if (io->read_modify_write) {
            io->msg->type = KV_MSG_SET;
            io->msg->value_len = opt.value_size;
            io->read_modify_write = false;
            kv_app_send(io->worker_id, io_start, io);
            return;
        }
    }
    if (p->start_io == p->end_io) {
        if (--p->iocnt == 0) {
            kv_app_send(opt.ssd_num, test_fini, NULL);
        }
        return;
    }
    io->read_modify_write = false;
    struct kv_msg *msg = io->msg;
    switch (state) {
        case SEQ_WRITE:
        case FILL:
            msg->type = KV_MSG_SET;
            msg->key_len = 16;
            msg->value_len = opt.value_size;
            kv_ycsb_next(workload, true, KV_MSG_KEY(msg), KV_MSG_VALUE(msg));
            break;
        case SEQ_READ:
            msg->type = KV_MSG_GET;
            msg->key_len = 16;
            msg->value_len = 0;
            kv_ycsb_next(workload, true, KV_MSG_KEY(msg), NULL);
            break;
        case TRANSACTION:
            do_transaction(io, msg);
            break;
        case INIT:
            assert(false);
    }
    io->is_finished = true;
    p->start_io++;
    gettimeofday(&io->io_start, NULL);
    io->worker_id = *(uint64_t *)(KV_MSG_KEY(msg) + 8) % opt.ssd_num;
    kv_app_send(io->worker_id, io_start, io);
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
    uint64_t value_log_block_num = self->storage.num_blocks * 0.95 - 2 * bucket_num;
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
    kv_ycsb_init(&workload, opt.workload_file, &opt.num_items, &opt.operation_cnt, &opt.value_size);
    gettimeofday(&tv_start, NULL);
    kv_app_start(opt.json_config_file, opt.ssd_num + opt.producer_num, task);
    kv_ycsb_fini(workload);
    kv_ds_queue_fini(&ds_queue);
    free(producers);
    free(workers);
    free(task);
    return 0;
}