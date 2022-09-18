#include <assert.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>

#include "../kv_app.h"
#include "../kv_data_store.h"
#include "../kv_memory.h"
#include "../kv_msg.h"
#include "../kv_ring.h"
#include "../utils/uthash.h"
struct {
    uint64_t num_items;
    uint32_t value_size;
    uint32_t ssd_num;
    uint32_t thread_num;
    uint32_t concurrent_io_num, copy_concurrency;
    uint32_t ring_num, vid_per_ssd, rpl_num;
    char json_config_file[1024];
    char etcd_ip[32];
    char etcd_port[16];
    char local_ip[32];
    char local_port[16];

} opt = {.ssd_num = 2,
         .thread_num = 1,
         .concurrent_io_num = 32,
         .copy_concurrency = 32,
         .rpl_num = 1,
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
    while ((ch = getopt(argc, argv, "hn:r:v:d:c:i:T:s:P:l:p:m:M:R:I:")) != -1) switch (ch) {
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
            case 'I':
                opt.copy_concurrency = atol(optarg);
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
                opt.ring_num = atol(optarg);
                break;
            case 'm':
                opt.vid_per_ssd = atol(optarg);
                break;
            case 'R':
                opt.rpl_num = atol(optarg);
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
kv_rdma_mrs_handle server_mrs;
struct io_ctx {
    void *req_h;
    struct kv_msg *msg;
    uint32_t worker_id;
    uint32_t server_thread;
    kv_rdma_mr req;
    void *fwd_ctx;
    bool has_next_node, need_forward, in_copy_pool;
    uint32_t vnode_type;
    kv_data_store_ctx ds_ctx;
    uint32_t msg_type;
    struct kv_data_store_copy_buf *copy_buf;
};

struct kv_mempool *io_pool, *copy_pool;
struct kv_ds_queue ds_queue;

static void send_response(void *arg) {
    struct io_ctx *io = arg;
    io->msg->q_info = ds_queue.q_info[io->worker_id];
    kv_rdma_make_resp(io->req_h, (uint8_t *)io->msg, KV_MSG_SIZE(io->msg));
    kv_mempool_put(io_pool, io);
}

static void forward_cb(void *arg) {
    struct io_ctx *io = arg;
    if (io->in_copy_pool) {
        if (io->msg->type == KV_MSG_OK) {
            struct kv_data_store_copy_buf *copy_buf = io->copy_buf;
            kv_mempool_put(copy_pool, io);
            kv_data_store_copy_commit(copy_buf);
        } else if (io->msg->type == KV_MSG_OUTDATED) {
            // retry
            io->msg->type = KV_MSG_SET;
            io->msg->value_len = io->copy_buf->val_len;
            kv_ring_forward(io->fwd_ctx, io->req, io->in_copy_pool, forward_cb, io);
        } else {
            fprintf(stderr, "kv_server: copy forward failed.\n");
            exit(-1);
        }
        return;
    }
    if (io->vnode_type == KV_RING_VNODE && (io->msg_type == KV_MSG_SET || io->msg_type == KV_MSG_DEL)) {
        struct kv_data_store *ds = &(workers + io->worker_id)->data_store;
        kv_data_store_clean(ds, KV_MSG_KEY(io->msg), io->msg->key_len);
    }
    if (io->msg_type == KV_MSG_SET) {
        kv_data_store_set_commit(io->ds_ctx, io->msg->type == KV_MSG_OK);
    } else if (io->msg_type == KV_MSG_DEL) {
        kv_data_store_del_commit(io->ds_ctx, io->msg->type == KV_MSG_OK);
    }
    kv_app_send(io->server_thread, send_response, arg);
}

static void io_fini(bool success, void *arg) {
    struct io_ctx *io = arg;
    if (io->in_copy_pool && success == false) {
        fprintf(stderr, "kv_server: copy failed.\n");
        exit(-1);
    }
    struct kv_data_store *ds = &(workers + io->worker_id)->data_store;
    if (!success) {
        io->msg->type = KV_MSG_ERR;
        kv_ring_forward(io->fwd_ctx, NULL, false, forward_cb, io);
        return;
    }
    if (io->msg_type == KV_MSG_SET || io->msg_type == KV_MSG_DEL) {
        if (io->vnode_type == KV_RING_TAIL && io->has_next_node) {
            io->need_forward = kv_data_store_copy_forward(ds, KV_MSG_KEY(io->msg));
            // kv_data_store_copy_range_counter(ds, KV_MSG_KEY(io->msg), false);
        } else if (io->vnode_type == KV_RING_VNODE)
            io->need_forward = true;
    }

    if (io->need_forward == false) {  // is the last node
        if (io->msg->type == KV_MSG_SET) io->msg->value_len = 0;
        io->msg->type = KV_MSG_OK;
    }
    kv_ring_forward(io->fwd_ctx, io->need_forward ? io->req : NULL, io->in_copy_pool, forward_cb, io);
}

static void io_start(void *arg) {
    struct io_ctx *io = arg;
    struct worker_t *self = workers + io->worker_id;
    io->need_forward = false;
    switch (io->msg->type) {
        case KV_MSG_DEL:
        case KV_MSG_SET:
            if (io->vnode_type == KV_RING_VNODE) kv_data_store_dirty(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len);
            // if (io->vnode_type == KV_RING_TAIL && io->next_node != NULL)
            //     kv_data_store_copy_range_counter(&self->data_store, KV_MSG_KEY(io->msg), true);
            if (io->msg->type == KV_MSG_SET)
                io->ds_ctx = kv_data_store_set(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len,
                                               KV_MSG_VALUE(io->msg), io->msg->value_len, io_fini, arg);
            else {
                assert(io->msg->value_len == 0);
                io->ds_ctx = kv_data_store_delete(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, io_fini, arg);
            }
            break;
        case KV_MSG_GET:
            if (kv_data_store_is_dirty(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len) && io->vnode_type == KV_RING_VNODE) {
                io->need_forward = true;
                io_fini(true, arg);
            } else {
                kv_data_store_get(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, KV_MSG_VALUE(io->msg),
                                  &io->msg->value_len, io_fini, arg);
            }
            break;
        case KV_MSG_TEST:
            io_fini(true, io);
            break;
        default:
            assert(false);
    }
}
struct server_copy_ctx {
    struct kv_ring_copy_info *info;
    bool is_start;
    uint32_t ds_id;
    uint64_t key_start;
    uint64_t key_end;
    bool del;
};
static void copy_fini(void *arg) {
    struct server_copy_ctx *ctx = arg;
    kv_ring_stop_copy(ctx->info);
    kv_free(ctx);
}
static void on_copy_fini(bool success, void *arg) {
    assert(success);
    kv_app_send(opt.ssd_num, copy_fini, arg);
}
static void on_copy_msg(void *arg) {
    struct server_copy_ctx *ctx = arg;
    struct worker_t *self = workers + ctx->ds_id;
    if (ctx->is_start) {
        kv_data_store_copy_add_key_range(&self->data_store, (uint8_t *)&ctx->key_start, (uint8_t *)&ctx->key_end, on_copy_fini, ctx);
    } else {
        kv_data_store_copy_del_key_range(&self->data_store, (uint8_t *)&ctx->key_start, (uint8_t *)&ctx->key_end, ctx->del);
        kv_free(ctx);
    }
}

static void ring_copy_cb(bool is_start, struct kv_ring_copy_info *info, void *arg) {
    struct server_copy_ctx *ctx = kv_malloc(sizeof(*ctx));
    *ctx = (struct server_copy_ctx){info, is_start, info->ds_id, info->start, info->end, info->del};
    kv_app_send(info->ds_id, on_copy_msg, ctx);
}

static void handler(void *req_h, kv_rdma_mr req, void *fwd_ctx, bool has_next_node, uint32_t ds_id, uint32_t vnode_type, void *arg) {
    uint32_t thread_id = kv_app_get_thread_index();
    struct io_ctx *io = kv_mempool_get(io_pool);
    assert(io);
    io->req_h = req_h;
    io->msg = (struct kv_msg *)kv_rdma_get_req_buf(req);
    io->worker_id = ds_id;
    io->server_thread = thread_id;
    io->req = req;
    io->fwd_ctx = fwd_ctx;
    io->has_next_node = has_next_node;
    io->in_copy_pool = false;
    io->vnode_type = vnode_type;
    io->msg_type = io->msg->type;
    kv_app_send(io->worker_id, io_start, io);
}

static void ring_init_cb(void *arg) {
    copy_pool = kv_mempool_create(opt.copy_concurrency, sizeof(struct io_ctx));
    server_mrs = kv_rdma_alloc_bulk(server, KV_RDMA_MR_SERVER, opt.value_size + sizeof(struct kv_msg) + KV_MAX_KEY_LENGTH, opt.copy_concurrency);
    for (size_t i = 0; i < opt.copy_concurrency; i++) {
        struct io_ctx *io = kv_mempool_get(copy_pool);
        io->req = kv_rdma_mrs_get(server_mrs, i);
        kv_mempool_put(copy_pool, io);
    }
}
static uint32_t io_cnt;
uint64_t log_bucket_num = 48;
static void ring_init(void *arg) {
    if (--io_cnt) return;
    server = kv_ring_init(opt.etcd_ip, opt.etcd_port, opt.thread_num, NULL, NULL);
    io_pool = kv_mempool_create(opt.concurrent_io_num, sizeof(struct io_ctx));
    kv_ring_server_init(opt.local_ip, opt.local_port, opt.ring_num, opt.vid_per_ssd, opt.ssd_num, opt.rpl_num,
                        log_bucket_num, opt.concurrent_io_num, sizeof(struct kv_msg) + KV_MAX_KEY_LENGTH + opt.value_size,
                        handler, NULL, ring_init_cb, NULL);
    kv_ring_register_copy_cb(ring_copy_cb, NULL);
}

static void copy_get_buf(uint8_t *key, uint8_t key_len, struct kv_data_store_copy_buf *buf, void *cb_arg) {
    struct io_ctx *io = kv_mempool_get(copy_pool);
    io->req_h = NULL;
    io->msg = (struct kv_msg *)kv_rdma_get_req_buf(io->req);
    io->worker_id = kv_app_get_thread_index();
    io->server_thread = opt.ssd_num + random() % opt.thread_num;
    io->msg->type = KV_MSG_SET;
    io->fwd_ctx = NULL;
    io->has_next_node = false;
    io->need_forward = true;
    io->in_copy_pool = true;
    io->msg->key_len = key_len;
    kv_memcpy(KV_MSG_KEY(io->msg), key, key_len);
    io->msg->value_len = buf->val_len;
    buf->val_buf = KV_MSG_VALUE(io->msg);
    buf->ctx = io;
    io->copy_buf = buf;
}

static void worker_init(void *arg) {
    struct worker_t *self = arg;
    kv_storage_init(&self->storage, self - workers);
    uint64_t bucket_num = opt.num_items / KV_ITEM_PER_BUCKET / opt.ssd_num;
    uint64_t value_log_block_num = self->storage.num_blocks * 0.95 - 2 * bucket_num;
    kv_data_store_init(&self->data_store, &self->storage, 0, bucket_num, log_bucket_num, value_log_block_num, 512, &ds_queue, self - workers);
    kv_data_store_copy_init(&self->data_store, copy_get_buf, NULL, opt.copy_concurrency, io_fini);
    kv_app_send(opt.ssd_num, ring_init, NULL);
}
#define KEY_PER_BKT_SEGMENT (KV_ITEM_PER_BUCKET)
// memory usage per key: 5/KEY_PER_BKT_SEGMENT bytes
int main(int argc, char **argv) {
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("DEBUG (low performance)\n");
#endif
    get_options(argc, argv);
    while ((1ULL << log_bucket_num) >= opt.num_items / KEY_PER_BKT_SEGMENT) log_bucket_num--;
    ++log_bucket_num;
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
    kv_ds_queue_init(&ds_queue, opt.ssd_num);
    kv_app_start(opt.json_config_file, opt.ssd_num + opt.thread_num, task);
    kv_ds_queue_fini(&ds_queue);
    free(workers);
    free(task);
    return 0;
}