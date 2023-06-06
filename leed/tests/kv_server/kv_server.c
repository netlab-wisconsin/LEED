#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../kv_app.h"
#include "../../kv_data_store.h"
#include "../../kv_msg.h"
#include "../../kv_rdma.h"

struct {
    uint64_t num_items;
    uint32_t value_size;
    uint32_t ssd_num;
    uint32_t server_num;
    uint32_t concurrent_io_num;
    char port[16];
    char json_config_file[1024];

} opt = {.ssd_num = 2, .server_num = 1, .concurrent_io_num = 32, .port = "9000", .json_config_file = "config.json"};
static void help(void) {
    // TODO: HELP TEXT
    printf("Some helpful text.\n");
    return;
}
static void get_options(int argc, char **argv) {
    int ch;
    while ((ch = getopt(argc, argv, "hn:r:v:d:c:i:s:p:")) != -1) switch (ch) {
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
            case 's':
                opt.server_num = atol(optarg);
                break;
            case 'p':
                strcpy(opt.port, optarg);
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
struct kv_ds_queue ds_queue;
struct io_ctx {
    kv_rdma_mr req_h;
    struct kv_msg *msg;
    uint32_t worker_id;
    uint32_t server_thread;
    kv_data_store_ctx ctx;
};

static void send_response(void *arg) {
    struct io_ctx *io = arg;
    kv_rdma_make_resp(io->req_h, (uint8_t *)io->msg, KV_MSG_SIZE(io->msg));
    free(io);
}

static void io_fini(bool success, void *arg) {
    struct io_ctx *io = arg;
    if (success) {
        if (io->msg->type == KV_MSG_SET) {
            kv_data_store_set_commit(io->ctx, true);
        } else if (io->msg->type == KV_MSG_DEL) {
            kv_data_store_del_commit(io->ctx, true);
        }
    }
    io->msg->type = success ? KV_MSG_OK : KV_MSG_ERR;
    kv_app_send(io->server_thread, send_response, arg);
}

static void io_start(void *arg) {
    struct io_ctx *io = arg;
    struct worker_t *self = workers + io->worker_id;
    switch (io->msg->type) {
        case KV_MSG_SET:
            io->ctx = kv_data_store_set(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, KV_MSG_VALUE(io->msg),
                                        io->msg->value_len, io_fini, arg);
            io->msg->value_len = 0;
            break;
        case KV_MSG_GET:
            kv_data_store_get(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, KV_MSG_VALUE(io->msg),
                              &io->msg->value_len, io_fini, arg);
            break;
        case KV_MSG_DEL:
            assert(io->msg->value_len == 0);
            io->ctx = kv_data_store_delete(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, io_fini, arg);
            break;
        case KV_MSG_TEST:
            io_fini(true, io);
            break;
        default:
            assert(false);
    }
}

static void handler(void *req_h, kv_rdma_mr req, uint32_t req_sz, void *arg) {
    struct io_ctx *ctx = malloc(sizeof(struct io_ctx));
    *ctx = (struct io_ctx){req_h, (struct kv_msg *)kv_rdma_get_req_buf(req), 0, kv_app_get_thread_index()};
    uint64_t key_frag = *(uint64_t *)(KV_MSG_KEY(ctx->msg) + 8);
    ctx->worker_id = key_frag % opt.ssd_num;
    kv_app_send(ctx->worker_id, io_start, ctx);
}
static uint32_t io_cnt;
static void rdma_start(void *arg) {
    if (--io_cnt) return;
    kv_rdma_init(&server, opt.server_num);
    kv_rdma_listen(server, "0.0.0.0", opt.port, opt.concurrent_io_num, sizeof(struct kv_msg) + 16 + opt.value_size, handler, NULL, NULL, NULL);
}

static void worker_init(void *arg) {
    struct worker_t *self = arg;
    kv_storage_init(&self->storage, self - workers);
    uint32_t bucket_num = opt.num_items / KV_ITEM_PER_BUCKET;
    uint64_t log_bucket_num = 48;
    while ((1ULL << log_bucket_num) >= opt.num_items / KV_ITEM_PER_BUCKET) log_bucket_num--;
    ++log_bucket_num;
    uint64_t value_log_block_num = opt.value_size * opt.num_items * 1.4 / self->storage.block_size;
    kv_data_store_init(&self->data_store, &self->storage, 0, bucket_num, log_bucket_num, value_log_block_num, 512, &ds_queue, self - workers);
    kv_app_send(opt.ssd_num, rdma_start, NULL);
}

int main(int argc, char **argv) {
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("DEBUG (low performance)\n");
#endif
    get_options(argc, argv);
    struct kv_app_task *task = calloc(opt.ssd_num + opt.server_num, sizeof(struct kv_app_task));
    workers = calloc(opt.ssd_num, sizeof(struct worker_t));
    for (size_t i = 0; i < opt.ssd_num; i++) {
        task[i].func = worker_init;
        task[i].arg = workers + i;
    }
    for (size_t i = 0; i < opt.server_num; i++) {
        task[opt.ssd_num + i] = (struct kv_app_task){NULL, NULL};
    }
    io_cnt = opt.ssd_num;
    kv_ds_queue_init(&ds_queue, opt.ssd_num);
    kv_app_start(opt.json_config_file, opt.ssd_num + opt.server_num, task);
    kv_ds_queue_fini(&ds_queue);
    free(workers);
    free(task);
    return 0;
}