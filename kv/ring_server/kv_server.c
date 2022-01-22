#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>

#include "../kv_app.h"
#include "../kv_data_store.h"
#include "../kv_msg.h"
#include "../kv_ring.h"
struct {
    uint64_t num_items;
    uint32_t value_size;
    uint32_t ssd_num;
    uint32_t thread_num;
    uint32_t concurrent_io_num;
    uint32_t vid_num, vid_per_ssd, r_num;
    char json_config_file[1024];
    char etcd_ip[32];
    char etcd_port[16];
    char local_ip[32];
    char local_port[16];

} opt = {.ssd_num = 2,
         .thread_num = 1,
         .concurrent_io_num = 32,
         .r_num = 1,
         .json_config_file = "config.json",
         .etcd_ip = "127.0.0.1",
         .etcd_port = "2379",
         .local_ip = "192.168.1.13",
         .local_port = "9000"};
static void help(void) {
    // TODO: HELP TEXT
    printf("Some helpful text.\n");
    return;
}
static void get_options(int argc, char **argv) {
    int ch;
    while ((ch = getopt(argc, argv, "hn:r:v:d:c:i:T:s:P:l:p:m:M:R:")) != -1) switch (ch) {
            case 'h':
                help();
                break;
            case 'n':
                opt.num_items = atoll(optarg);
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
            case 'T':
                opt.thread_num = atol(optarg);
                break;
            case 's':
                strcpy(opt.etcd_ip, optarg);
                break;
            case 'P':
                strcpy(opt.etcd_port, optarg);
                break;
            case 'l':
                strcpy(opt.local_ip, optarg);
                break;
            case 'p':
                strcpy(opt.local_port, optarg);
                break;
            case 'M':
                opt.vid_num = atol(optarg);
                break;
            case 'm':
                opt.vid_per_ssd = atol(optarg);
                break;
            case 'R':
                opt.r_num = atol(optarg);
                break;
            default:
                help();
                exit(-1);
        }
}

struct worker_t {
    struct kv_storage storage;
    struct kv_data_store data_store;
} * workers;

kv_rdma_handle server;
struct io_ctx {
    void *req;
    struct kv_msg *msg;
    uint32_t worker_id;
    uint32_t server_thread;
    void *resp;
    SLIST_ENTRY(io_ctx) next;
};

SLIST_HEAD(, io_ctx) * io_ctx_heads;

static void send_response(void *arg) {
    struct io_ctx *io = arg;
    kv_rdma_make_resp(io->req, (uint8_t *)io->msg, KV_MSG_SIZE(io->msg));
    SLIST_INSERT_HEAD(io_ctx_heads + io->server_thread, io, next);
}

static void request_cb(connection_handle h, bool success, kv_rmda_mr req, kv_rmda_mr resp, void *arg) {
    struct io_ctx *io = arg;
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_resp_buf(resp);
    io->msg->type = success && msg->type == KV_MSG_OK ? KV_MSG_OK : KV_MSG_ERR;
    kv_app_send(io->server_thread, send_response, arg);
}

static void forward_request(void *arg) {
    struct io_ctx *io = arg;
    connection_handle h;
    kv_ring_forward(KV_MSG_KEY(io->msg), io->msg->hop, opt.r_num, &h, &io->msg->ssd_id);
    if (h) {
        io->msg->hop++;
        kv_rmda_send_req(h, io->req, KV_MSG_SIZE(io->msg), io->resp, request_cb, io);
    } else {
        send_response(io);
    }
}

static void io_fini(bool success, void *arg) {
    struct io_ctx *io = arg;
    if (success && (io->msg->type == KV_MSG_SET || io->msg->type == KV_MSG_DEL)) {
        kv_app_send(io->server_thread, forward_request, arg);
    } else {
        io->msg->type = success ? KV_MSG_OK : KV_MSG_ERR;
        kv_app_send(io->server_thread, send_response, arg);
    }
}

static void io_start(void *arg) {
    struct io_ctx *io = arg;
    struct worker_t *self = workers + io->worker_id;
    switch (io->msg->type) {
        case KV_MSG_SET:
            kv_data_store_set(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, KV_MSG_VALUE(io->msg),
                              io->msg->value_len, io_fini, arg);
            io->msg->value_len = 0;
            break;
        case KV_MSG_GET:
            kv_data_store_get(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, KV_MSG_VALUE(io->msg),
                              &io->msg->value_len, io_fini, arg);
            break;
        case KV_MSG_DEL:
            assert(io->msg->value_len == 0);
            kv_data_store_delete(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, io_fini, arg);
            break;
        case KV_MSG_TEST:
            io_fini(true, io);
            break;
        default:
            assert(false);
    }
}

static void handler(void *req, uint8_t *buf, uint32_t req_sz, void *arg) {
    uint32_t thread_id = kv_app_get_thread_index();
    struct io_ctx *ctx = SLIST_FIRST(io_ctx_heads + thread_id - opt.ssd_num);
    assert(ctx);
    SLIST_REMOVE_HEAD(io_ctx_heads + thread_id - opt.ssd_num, next);
    *ctx = (struct io_ctx){req, (struct kv_msg *)buf, 0, thread_id};
    ctx->worker_id = ctx->msg->ssd_id;
    kv_app_send(ctx->worker_id, io_start, ctx);
}
#define EXTRA_BUF 32
static void ring_ready_cb(void *arg) {
    io_ctx_heads = calloc(opt.thread_num, sizeof(*io_ctx_heads));
    for (size_t i = 0; i < opt.thread_num; i++) {
        SLIST_INIT(io_ctx_heads + i);
        for (size_t j = 0; j < opt.concurrent_io_num; j++) {
            struct io_ctx *ctx = malloc(sizeof(struct io_ctx));
            ctx->resp = kv_rdma_alloc_resp(server, sizeof(struct kv_msg) + KV_MAX_KEY_LENGTH);
            SLIST_INSERT_HEAD(io_ctx_heads + i, ctx, next);
        }
    }
    kv_ring_server_init(opt.local_ip, opt.local_port, opt.vid_num, opt.vid_per_ssd, opt.ssd_num, opt.concurrent_io_num,
                        EXTRA_BUF + opt.value_size, handler, NULL);
}

static void ring_init(void *arg) { kv_ring_init(opt.etcd_ip, opt.etcd_port, opt.thread_num, ring_ready_cb, NULL); }

static uint32_t io_cnt;
static void worker_init_done(bool success, void *arg) {
    if (!success) {
        fprintf(stderr, "init fail!\n");
        exit(-1);
    }
    if (--io_cnt == 0) {
        kv_app_send(opt.ssd_num, ring_init, NULL);
    }
}

static void worker_init(void *arg) {
    struct worker_t *self = arg;
    kv_storage_init(&self->storage, self - workers);
    uint32_t bucket_num = opt.num_items / KV_ITEM_PER_BUCKET;
    uint64_t value_log_block_num = opt.value_size * opt.num_items * 1.4 / self->storage.block_size;
    kv_data_store_init(&self->data_store, &self->storage, 0, bucket_num, value_log_block_num, 512, worker_init_done, NULL);
}

int main(int argc, char **argv) {
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("DEBUG (low performance)\n");
#endif
    get_options(argc, argv);
    struct kv_app_task *task = calloc(opt.ssd_num + opt.thread_num, sizeof(struct kv_app_task));
    workers = calloc(opt.ssd_num, sizeof(struct worker_t));
    for (size_t i = 0; i < opt.ssd_num; i++) {
        task[i].func = worker_init;
        task[i].arg = workers + i;
    }
    for (size_t i = 0; i < opt.thread_num; i++) {
        task[opt.ssd_num + i] = (struct kv_app_task){NULL, NULL};
    }
    io_cnt = opt.ssd_num;
    kv_app_start(opt.json_config_file, opt.ssd_num + opt.thread_num, task);
    free(workers);
    free(task);
    return 0;
}