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
struct {
    uint64_t num_items, read_num_items;
    uint32_t value_size;
    uint32_t thread_num;
    uint32_t producer_num;
    uint32_t concurrent_io_num;
    char json_config_file[1024];
    char server_ip[32];
    char server_port[16];
} opt = {
    .num_items = 1024,
    .read_num_items = 512,
    .thread_num = 2,
    .producer_num = 1,
    .value_size = 1024,
    .concurrent_io_num = 32,
    .json_config_file = "config.json",
    .server_ip = "192.168.1.13",
};
static void help(void) {
    // TODO: HELP TEXT
    printf("Some helpful text.\n");
    return;
}
static void get_options(int argc, char **argv) {
    int ch;
    while ((ch = getopt(argc, argv, "htn:r:v:P:c:i:p:s:T:")) != -1) switch (ch) {
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
            default:
                help();
                exit(-1);
        }
}

static inline uint128 index_to_key(uint64_t index) { return CityHash128((char *)&index, sizeof(uint64_t)); }
struct io_buffer_t {
    kv_rmda_mr req, resp;
    uint32_t producer_id;
    bool is_finished;
} * io_buffers;

struct producer_t {
    uint64_t start_io, end_io;
    uint64_t iocnt;
} * producers;

static struct timeval tv_start, tv_end;
enum { INIT, FILL, READ, CLEAR } state = INIT;
kv_rdma_handle rdma;
kv_rmda_mrs_handle req_mrs, resp_mrs;

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
static void test_fini(void *arg) {  // always running on producer 0
    uint64_t total_io = 0;
    static uint32_t producer_cnt = 1;
    if (--producer_cnt) return;
    gettimeofday(&tv_end, NULL);
    producer_cnt = opt.producer_num;
    switch (state) {
        case INIT:
            req_mrs = kv_rdma_alloc_bulk(rdma, KV_RDMA_MR_REQ, opt.value_size + sizeof(struct kv_msg) + 16, opt.concurrent_io_num);
            resp_mrs = kv_rdma_alloc_bulk(rdma, KV_RDMA_MR_RESP, opt.value_size + sizeof(struct kv_msg) + 16, opt.concurrent_io_num);
            for (size_t i = 0; i < opt.concurrent_io_num; i++) {
                io_buffers[i].req = kv_rdma_mrs_get(req_mrs, i);
                io_buffers[i].resp = kv_rdma_mrs_get(resp_mrs, i);
            }
            printf("rdma client initialized in %lf s.\n", timeval_diff(&tv_start, &tv_end));
            total_io = opt.num_items;
            state = FILL;
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
    for (size_t i = 0; i < opt.concurrent_io_num; i++) io_buffers[i].is_finished = false;
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

static void test(void *arg) {
    struct io_buffer_t *io = arg;
    if (io && io->is_finished) {
        if (((struct kv_msg *)kv_rdma_get_resp_buf(io->resp))->type != KV_MSG_OK) {
            fprintf(stderr, "io fail. \n");
            exit(-1);
        }
    }
    struct producer_t *p = io ? producers + io->producer_id : producers;
    if (p->start_io == p->end_io) {
        if (--p->iocnt == 0) {
            kv_app_send(opt.thread_num, test_fini, NULL);
        }
        return;
    }
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(io->req);
    switch (state) {
        case FILL:
            msg->type = KV_MSG_SET;
            *(uint128 *)KV_MSG_KEY(msg) = index_to_key(p->start_io);
            msg->key_len = 16;
            msg->value_len = opt.value_size;
            break;
        case READ:
            msg->type = KV_MSG_GET;
            *(uint128 *)KV_MSG_KEY(msg) = index_to_key(random() % opt.num_items);
            msg->key_len = 16;
            msg->value_len = 0;
            break;
        case CLEAR:
            msg->type = KV_MSG_DEL;
            *(uint128 *)KV_MSG_KEY(msg) = index_to_key(p->start_io);
            msg->key_len = 16;
            msg->value_len = 0;
            break;
        case INIT:
            assert(false);
    }
    p->start_io++;
    io->is_finished = true;
    kv_ring_dispatch(io->req, io->resp, kv_rdma_get_resp_buf(io->resp), test, io);
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
    gettimeofday(&tv_start, NULL);
    kv_app_start(opt.json_config_file, opt.thread_num + opt.producer_num, task);
    free(io_buffers);
    free(producers);
    free(task);
}