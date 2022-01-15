#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "../benchmark2/city.h"
#include "../benchmark2/timing.h"
#include "../kv_app.h"
#include "../kv_msg.h"
#include "../kv_rdma.h"
struct {
    uint64_t num_items, read_num_items;
    uint32_t value_size;
    uint32_t client_num;
    uint32_t thread_num;
    uint32_t producer_num;
    uint32_t concurrent_io_num;
    char json_config_file[1024];
    char server_ip[32];
    char server_port[16];
    bool test_rdma;
} opt = {.num_items = 1024,
         .read_num_items = 512,
         .client_num = 2,
         .thread_num = 2,
         .producer_num = 1,
         .value_size = 1024,
         .concurrent_io_num = 32,
         .json_config_file = "config.json",
         .server_ip = "192.168.1.13",
         .test_rdma = false};
static void help(void) {
    // TODO: HELP TEXT
    printf("Some helpful text.\n");
    return;
}
static void get_options(int argc, char **argv) {
    int ch;
    while ((ch = getopt(argc, argv, "htn:r:v:P:c:i:p:s:T:C:")) != -1) switch (ch) {
            case 'h':
                help();
                break;
            case 't':
                opt.test_rdma = true;
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
            case 'p':
                strcpy(opt.server_port, optarg);
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
            case 'C':
                opt.client_num = atol(optarg);
                break;
            case 'T':
                opt.thread_num = atol(optarg);
                break;
            case 's':
                strcpy(opt.server_ip, optarg);
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
    uint32_t client_id;
    uint32_t producer_id;
} * io_buffers;

struct client_t {
    // char *ip;
    // char *port;
    connection_handle h;
} * clients;

struct producer_t {
    uint64_t start_io, end_io;
    uint64_t iocnt;
} * producers;

static struct timeval tv_start, tv_end;
enum { INIT, FILL, READ, CLEAR, TEST } state = INIT;
kv_rdma_handle rdma;
static void thread_stop(void *arg) { kv_app_stop(0); }
static void rdma_fini_cb(void *arg) {
    for (size_t i = 0; i < opt.thread_num + opt.producer_num; i++) kv_app_send(i, thread_stop, NULL);
}
static void disconnect_cb(void *arg) {
    if (--opt.client_num == 0) {
        kv_rdma_fini(rdma, rdma_fini_cb, NULL);
    }
}

static void stop(void) {
    for (size_t i = 0; i < opt.concurrent_io_num; i++) {
        kv_rdma_free_mr(io_buffers[i].req);
        kv_rdma_free_mr(io_buffers[i].resp);
    }
    for (size_t i = 0; i < opt.client_num; i++) kv_rdma_disconnect(clients[i].h);
}

static void test(void *arg);
static void io_fini(connection_handle h, bool success, kv_rmda_mr req, kv_rmda_mr resp, void *arg) {
    struct io_buffer_t *io = arg;
    if (!success) {
        fprintf(stderr, "io fail. \n");
        exit(-1);
    }
    kv_app_send(opt.thread_num + io->producer_id, test, arg);
}
static void io_start(void *arg) {
    struct io_buffer_t *io = arg;
    struct client_t *client = clients + io->client_id;
    kv_rmda_send_req(client->h, io->req, io->req_sz, io->resp, io_fini, arg);
}

static void test_fini(void *arg) {  // always running on producer 0
    uint64_t total_io = 0;
    static uint32_t producer_cnt = 1;
    if (--producer_cnt) return;
    gettimeofday(&tv_end, NULL);
    producer_cnt = opt.producer_num;
    switch (state) {
        case INIT:
            printf("rdma client initialized in %lf s.\n", timeval_diff(&tv_start, &tv_end));
            for (size_t i = 0; i < opt.concurrent_io_num; i++) {
                io_buffers[i].req = kv_rdma_alloc_req(rdma, opt.value_size + EXTRA_BUF);
                io_buffers[i].resp = kv_rdma_alloc_resp(rdma, opt.value_size + EXTRA_BUF);
            }
            total_io = opt.num_items;
            state = opt.test_rdma ? TEST : FILL;
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
        case TEST:
            printf("Request rate: %f\n", ((double)opt.num_items / timeval_diff(&tv_start, &tv_end)));
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

static void test(void *arg) {
    struct io_buffer_t *io = arg;
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
            msg->value_offset = msg->key_len;
            msg->value_len = opt.value_size;
            io->req_sz = KV_MSG_SIZE(msg);
            break;
        case READ:
            msg->type = KV_MSG_GET;
            *(uint128 *)KV_MSG_KEY(msg) = index_to_key(random() % opt.num_items);
            msg->key_len = 16;
            msg->value_offset = msg->key_len;
            msg->value_len = 0;
            io->req_sz = KV_MSG_SIZE(msg);
            break;
        case CLEAR:
            msg->type = KV_MSG_DEL;
            *(uint128 *)KV_MSG_KEY(msg) = index_to_key(p->start_io);
            msg->key_len = 16;
            msg->value_offset = msg->key_len;
            msg->value_len = 0;
            io->req_sz = KV_MSG_SIZE(msg);
            break;
        case TEST:
            msg->type = KV_MSG_TEST;
            *(uint128 *)KV_MSG_KEY(msg) = (uint128){0, 0};
            msg->key_len = 16;
            msg->value_offset = msg->key_len;
            msg->value_len = opt.value_size;
            io->req_sz = EXTRA_BUF;
            break;
        case INIT:
            assert(false);
    }
    p->start_io++;
    io->client_id = random() % opt.client_num;
    kv_app_send(random() % opt.thread_num, io_start, arg);
}

static void send_init_done_msg(connection_handle h, void *arg) {
    if (!h) {
        fprintf(stderr, "connect fail!\n");
        exit(-1);
    }
    struct client_t *client = arg;
    client->h = h;
    kv_app_send(opt.thread_num, test, NULL);
}
static void rdma_init(void *arg) {
    kv_rdma_init(&rdma, opt.thread_num);
    for (size_t i = 0; i < opt.client_num; i++)
        kv_rdma_connect(rdma, opt.server_ip, opt.server_port, send_init_done_msg, clients + i, disconnect_cb, NULL);
}

int main(int argc, char **argv) {
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("DEBUG (low performance)\n");
#endif
    get_options(argc, argv);
    struct kv_app_task *task = calloc(opt.thread_num + opt.producer_num, sizeof(struct kv_app_task));
    io_buffers = calloc(opt.concurrent_io_num, sizeof(struct io_buffer_t));
    clients = calloc(opt.client_num, sizeof(struct client_t));
    for (size_t i = 0; i < opt.thread_num; i++) {
        task[i] = (struct kv_app_task){NULL, NULL};
    }
    task[0].func = rdma_init;
    producers = calloc(opt.producer_num, sizeof(struct producer_t));
    for (size_t i = 0; i < opt.producer_num; i++) {
        task[opt.thread_num + i] = (struct kv_app_task){NULL, NULL};
    }
    *producers = (struct producer_t){0, 0, opt.client_num};
    gettimeofday(&tv_start, NULL);
    kv_app_start(opt.json_config_file, opt.thread_num + opt.producer_num, task);
    free(io_buffers);
    free(clients);
    free(producers);
    free(task);
}