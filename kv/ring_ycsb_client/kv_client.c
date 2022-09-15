#include <assert.h>
#include <getopt.h>
#include <math.h>
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
    int stat_interval;
    bool seq_read, fill, seq_write, del;
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
         .stat_interval = -1,
         .json_config_file = "config.json",
         .server_ip = "192.168.1.13",
         .seq_read = false,
         .seq_write = false,
         .del = false,
         .fill = false};
static void help(void) {
    // TODO: HELP TEXT
    printf("Some helpful text.\n");
    return;
}
static void get_options(int argc, char **argv) {
    int ch;
    while ((ch = getopt(argc, argv, "htn:r:v:P:c:i:p:s:T:w:I:RWFD")) != -1) switch (ch) {
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
            case 'I':
                opt.stat_interval = atol(optarg);
                break;
            case 'R':
                opt.seq_read = true;
                break;
            case 'W':
                opt.seq_write = true;
                break;
            case 'D':
                opt.del = true;
                break;
            case 'F':
                opt.fill = true;
                break;
            default:
                help();
                exit(-1);
        }
}

struct io_buffer_t {
    kv_rdma_mr req, resp;
    uint32_t producer_id;
    bool read_modify_write, is_finished;
    struct timeval io_start;
} * io_buffers;

struct producer_t {
    uint64_t start_io, end_io;
    uint64_t iocnt;
    double latency_sum;
    uint32_t io_per_record;
    _Atomic uint64_t counter;  // for real time thourghput
} * producers;

static void *tp_poller = NULL;
#define LATENCY_MAX_RECORD 0x100000  // 1M
static double latency_records[LATENCY_MAX_RECORD];

static struct timeval tv_start, tv_end;
enum {
    INIT,
    FILL,
    TRANSACTION,
    SEQ_READ,
    SEQ_WRITE,
    DEL,
} state = INIT;
kv_rdma_handle rdma;
kv_rdma_mrs_handle req_mrs, resp_mrs;
kv_ycsb_handle workload;

static void thread_stop(void *arg) { kv_app_stop(0); }
static void ring_fini_cb(void *arg) {
    for (size_t i = 0; i < opt.thread_num + opt.producer_num; i++) kv_app_send(i, thread_stop, NULL);
}
static void stop(void) {
    kv_rdma_free_bulk(req_mrs);
    kv_rdma_free_bulk(resp_mrs);
    if (tp_poller) kv_app_poller_unregister(&tp_poller);
    kv_ring_fini(ring_fini_cb, NULL);
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
            req_mrs = kv_rdma_alloc_bulk(rdma, KV_RDMA_MR_REQ, opt.value_size + sizeof(struct kv_msg) + 16, opt.concurrent_io_num);
            resp_mrs =
                kv_rdma_alloc_bulk(rdma, KV_RDMA_MR_RESP, opt.value_size + sizeof(struct kv_msg) + 16, opt.concurrent_io_num);
            for (size_t i = 0; i < opt.concurrent_io_num; i++) {
                io_buffers[i].req = kv_rdma_mrs_get(req_mrs, i);
                io_buffers[i].resp = kv_rdma_mrs_get(resp_mrs, i);
            }
            printf("rdma client initialized in %lf s.\n", timeval_diff(&tv_start, &tv_end));
            if (opt.fill) {
                total_io = opt.num_items;
                state = FILL;
            } else {
                state = opt.del ? DEL : opt.seq_read ? SEQ_READ
                                    : opt.seq_write  ? SEQ_WRITE
                                                     : TRANSACTION;
                total_io = opt.operation_cnt;
            }
            break;
        case FILL:
            printf("Write rate: %lf\n", ((double)opt.num_items / timeval_diff(&tv_start, &tv_end)));
            puts("db created successfully.");
            state = opt.del ? DEL : opt.seq_read ? SEQ_READ
                                : opt.seq_write  ? SEQ_WRITE
                                                 : TRANSACTION;
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
        case DEL:
            printf("DEL rate: %lf\n", ((double)opt.operation_cnt / timeval_diff(&tv_start, &tv_end)));
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
            kv_app_send(opt.thread_num + i, test, io_buffers + j);
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
    struct producer_t *p = io ? producers + io->producer_id : producers;
    if (io && io->is_finished) {
        p->counter++;
        struct timeval io_end;
        gettimeofday(&io_end, NULL);
        double latency = timeval_diff(&io->io_start, &io_end);
        p->latency_sum += latency;
        if (p->start_io % p->io_per_record == 0) {
            latency_records[p->start_io / p->io_per_record] = latency;
        }
        if (((struct kv_msg *)kv_rdma_get_resp_buf(io->resp))->type != KV_MSG_OK) {
            fprintf(stderr, "io fail. \n");
            exit(-1);
        } else if (io->read_modify_write) {
            struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(io->req);
            msg->type = KV_MSG_SET;
            msg->value_len = opt.value_size;
            io->read_modify_write = false;
            kv_ring_dispatch(io->req, io->resp, kv_rdma_get_resp_buf(io->resp), test, io);
            return;
        }
    }

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
            msg->value_len = opt.value_size;
            kv_ycsb_next(workload, true, KV_MSG_KEY(msg), KV_MSG_VALUE(msg));
            break;
        case SEQ_READ:
            msg->type = KV_MSG_GET;
            msg->key_len = 16;
            msg->value_len = 0;
            kv_ycsb_next(workload, true, KV_MSG_KEY(msg), NULL);
            break;
        case DEL:
            msg->type = KV_MSG_DEL;
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
    kv_ring_dispatch(io->req, io->resp, kv_rdma_get_resp_buf(io->resp), test, io);
}
static struct timeval stat_tv;
static int throughput_poller(void *arg) {
    struct timeval now;
    gettimeofday(&now, NULL);
    double duration = timeval_diff(&stat_tv, &now);
    stat_tv = now;
    uint64_t sum = 0;
    for (size_t i = 0; i < opt.producer_num; i++) {
        sum += atomic_exchange(&producers[i].counter, 0);
    }
    printf("throughput: %lf IOPS\n", (double)sum / duration);
    return 0;
}

static void ring_ready_cb(void *arg) { kv_app_send(opt.thread_num, test, NULL); }
static void ring_init(void *arg) {
    gettimeofday(&stat_tv, NULL);
    if (opt.stat_interval > 0)
        tp_poller = kv_app_poller_register(throughput_poller, NULL, opt.stat_interval * 1000);
    rdma = kv_ring_init(opt.server_ip, opt.server_port, opt.thread_num, ring_ready_cb, NULL);
}

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