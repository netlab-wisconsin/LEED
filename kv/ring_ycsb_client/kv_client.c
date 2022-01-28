#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "../kv_app.h"
#include "../kv_msg.h"
#include "../kv_ring.h"
#include "../utils/city.h"
#include "../utils/timing.h"
#include "../ycsb/kv_ycsb.h"

struct {
    uint64_t num_items, operation_cnt;
    uint32_t value_size;
    uint32_t thread_num;
    uint32_t producer_num;
    uint32_t concurrent_io_num;
    bool seq_read, fill, seq_write;
    char json_config_file[1024];
    char workload_file[1024];
    char server_ip[32];
    char server_port[16];
} opt = {.num_items = 1024,
         .operation_cnt = 512,
         .thread_num = 2,
         .producer_num = 1,
         .value_size = 1024,
         .concurrent_io_num = 32,
         .json_config_file = "config.json",
         .server_ip = "192.168.1.13",
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
    while ((ch = getopt(argc, argv, "htn:r:v:P:c:i:p:s:T:w:RWF")) != -1) switch (ch) {
            case 'h':
                help();
                break;
            case 'w':
                strcpy(opt.workload_file, optarg);
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
            case 'T':
                opt.thread_num = atol(optarg);
                break;
            case 's':
                strcpy(opt.server_ip, optarg);
                break;
            case 'p':
                strcpy(opt.server_port, optarg);
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

static inline uint128 index_to_key(uint64_t index) { return CityHash128((char *)&index, sizeof(uint64_t)); }
#define EXTRA_BUF 32
struct io_buffer_t {
    kv_rmda_mr req, resp;
    uint32_t req_sz;
    uint32_t producer_id;
    connection_handle h;
    bool read_modify_write;
} * io_buffers;

struct producer_t {
    uint64_t start_io, end_io;
    uint64_t iocnt;
} * producers;

static struct timeval tv_start, tv_end;
enum {
    INIT,
    FILL,
    TRANSACTION,
    SEQ_READ,
    SEQ_WRITE,
} state = INIT;
kv_rdma_handle rdma;
kv_rmda_mrs_handle req_mrs, resp_mrs;
kv_ycsb_handle workload;

static void thread_stop(void *arg) { kv_app_stop(0); }
static void ring_fini_cb(void *arg) {
    for (size_t i = 0; i < opt.thread_num + opt.producer_num; i++) kv_app_send(i, thread_stop, NULL);
}
static void stop(void) {
    kv_rdma_free_bulk(req_mrs);
    kv_rdma_free_bulk(resp_mrs);
    kv_ring_fini(ring_fini_cb, NULL);
}

static void test(void *arg);
static void modify_write(void *arg);
static void io_fini(connection_handle h, bool success, kv_rmda_mr req, kv_rmda_mr resp, void *arg) {
    struct io_buffer_t *io = arg;
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_resp_buf(resp);
    if (!success || msg->type != KV_MSG_OK) {
        fprintf(stderr, "io fail. \n");
        exit(-1);
    }

    kv_app_send(opt.thread_num + io->producer_id, io->read_modify_write ? modify_write : test, arg);
}
static void io_start(void *arg) {
    struct io_buffer_t *io = arg;
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(io->req);
    kv_ring_dispatch(KV_MSG_KEY(msg), &io->h, &msg->ds_id);
    msg->hop = 1;
    kv_rmda_send_req(io->h, io->req, io->req_sz, io->resp, NULL, io_fini, arg);
}

static void test_fini(void *arg) {  // always running on producer 0
    uint64_t total_io = 0;
    static uint32_t producer_cnt = 1;
    if (--producer_cnt) return;
    gettimeofday(&tv_end, NULL);
    producer_cnt = opt.producer_num;
    switch (state) {
        case INIT:
            req_mrs = kv_rdma_alloc_bulk(rdma, KV_RDMA_MR_REQ, opt.value_size + EXTRA_BUF, opt.concurrent_io_num);
            resp_mrs = kv_rdma_alloc_bulk(rdma, KV_RDMA_MR_RESP, opt.value_size + EXTRA_BUF, opt.concurrent_io_num);
            for (size_t i = 0; i < opt.concurrent_io_num; i++) {
                io_buffers[i].req = kv_rdma_mrs_get(req_mrs, i);
                io_buffers[i].resp = kv_rdma_mrs_get(resp_mrs, i);
            }
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
            printf("Write rate: %f\n", ((double)opt.num_items / timeval_diff(&tv_start, &tv_end)));
            puts("db created successfully.");
            state = opt.seq_read ? SEQ_READ : opt.seq_write ? SEQ_WRITE : TRANSACTION;
            total_io = opt.operation_cnt;
            break;
        case SEQ_WRITE:
            printf("SEQ_WRITE rate: %f\n", ((double)opt.operation_cnt / timeval_diff(&tv_start, &tv_end)));
            stop();
            return;
        case SEQ_READ:
            printf("SEQ_READ rate: %f\n", ((double)opt.operation_cnt / timeval_diff(&tv_start, &tv_end)));
            stop();
            return;
        case TRANSACTION:
            printf("TRANSACTION rate: %f\n", ((double)opt.operation_cnt / timeval_diff(&tv_start, &tv_end)));
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
            io_buffers[j].producer_id = i;
            kv_app_send(opt.thread_num + i, test, io_buffers + j);
        }
    }
}
static void modify_write(void *arg) {
    struct io_buffer_t *io = arg;
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(io->req);
    msg->type = KV_MSG_SET;
    msg->value_len = opt.value_size;
    io->req_sz = KV_MSG_SIZE(msg);
    kv_app_send(random() % opt.thread_num, io_start, arg);
}
static inline void do_transaction(struct io_buffer_t *io, struct kv_msg *msg) {
    msg->key_len = 16;
    msg->value_offset = msg->key_len;
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
    io->req_sz = KV_MSG_SIZE(msg);
}

static void test(void *arg) {
    struct io_buffer_t *io = arg;
    struct producer_t *p = io ? producers + io->producer_id : producers;
    if (p->start_io == p->end_io) {
        if (--p->iocnt == 0) {
            kv_app_send(opt.thread_num, test_fini, NULL);
        }
        return;
    }
    io->read_modify_write = false;
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(io->req);
    switch (state) {
        case SEQ_WRITE:
        case FILL:
            msg->type = KV_MSG_SET;
            msg->key_len = 16;
            msg->value_offset = msg->key_len;
            msg->value_len = opt.value_size;
            kv_ycsb_next(workload, true, KV_MSG_KEY(msg), KV_MSG_VALUE(msg));
            break;
        case SEQ_READ:
            msg->type = KV_MSG_GET;
            msg->key_len = 16;
            msg->value_offset = msg->key_len;
            msg->value_len = 0;
            kv_ycsb_next(workload, true, KV_MSG_KEY(msg), NULL);
            break;
        case TRANSACTION:
            do_transaction(io, msg);
            break;
        case INIT:
            assert(false);
    }
    io->req_sz = KV_MSG_SIZE(msg);
    p->start_io++;
    kv_app_send(random() % opt.thread_num, io_start, arg);
}

static void ring_ready_cb(void *arg) { kv_app_send(opt.thread_num, test, NULL); }
static void ring_init(void *arg) { rdma = kv_ring_init(opt.server_ip, opt.server_port, opt.thread_num, ring_ready_cb, NULL); }

int main(int argc, char **argv) {
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("DEBUG (low performance)\n");
#endif
    get_options(argc, argv);
    io_buffers = calloc(opt.concurrent_io_num, sizeof(struct io_buffer_t));
    struct kv_app_task *task = calloc(opt.thread_num + opt.producer_num, sizeof(struct kv_app_task));
    for (size_t i = 0; i < opt.thread_num + opt.producer_num; i++) {
        task[i] = (struct kv_app_task){NULL, NULL};
    }
    task[0].func = ring_init;
    producers = calloc(opt.producer_num, sizeof(struct producer_t));
    *producers = (struct producer_t){0, 0, 1};
    kv_ycsb_init(&workload, opt.workload_file, &opt.num_items, &opt.operation_cnt, &opt.value_size);
    gettimeofday(&tv_start, NULL);
    kv_app_start(opt.json_config_file, opt.thread_num + opt.producer_num, task);
    kv_ycsb_fini(workload);
    free(io_buffers);
    free(producers);
    free(task);
}