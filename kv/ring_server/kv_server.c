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
    uint32_t concurrent_io_num;
    uint32_t ring_num, vid_per_ssd, rpl_num;
    char json_config_file[1024];
    char etcd_ip[32];
    char etcd_port[16];
    char local_ip[32];
    char local_port[16];

} opt = {.ssd_num = 2,
         .thread_num = 1,
         .concurrent_io_num = 32,
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

struct key_set_t {
    uint8_t key[KV_MAX_KEY_LENGTH];
    uint32_t cnt;
    UT_hash_handle hh;
};
struct worker_t {
    struct kv_storage storage;
    struct kv_data_store data_store;
    struct key_set_t *dirty_keys;
} * workers;

kv_rdma_handle server;
struct io_ctx {
    void *req_h;
    struct kv_msg *msg;
    uint32_t worker_id;
    uint32_t server_thread;
    kv_rdma_mr req;
    bool need_forward;
    void *next_node;
    kv_data_store_ctx ds_ctx;
    uint32_t msg_type;
};

struct kv_mempool *io_pool;
struct kv_ds_queue ds_queue;
static inline struct key_set_t *find_key(struct worker_t *worker, uint8_t *_key, uint8_t key_length) {
    uint8_t key[KV_MAX_KEY_LENGTH];
    kv_memset(key, 0, KV_MAX_KEY_LENGTH);
    kv_memcpy(key, _key, key_length);
    struct key_set_t *entry;
    HASH_FIND(hh, worker->dirty_keys, key, KV_MAX_KEY_LENGTH, entry);
    return entry;
}

static void put_key(struct worker_t *worker, uint8_t *key, uint8_t key_length) {
    struct key_set_t *entry = find_key(worker, key, key_length);
    if (entry) {
        entry->cnt++;
        return;
    }
    entry = kv_malloc(sizeof(struct key_set_t));
    kv_memset(entry->key, 0, KV_MAX_KEY_LENGTH);
    kv_memcpy(entry->key, key, key_length);
    entry->cnt = 1;
    HASH_ADD(hh, worker->dirty_keys, key, KV_MAX_KEY_LENGTH, entry);
}

static void del_key(struct io_ctx *io) {
    struct worker_t *worker = workers + io->worker_id;
    struct key_set_t *entry = find_key(worker, KV_MSG_KEY(io->msg), io->msg->key_len);
    assert(entry);
    if (--entry->cnt == 0) {
        HASH_DEL(worker->dirty_keys, entry);
        kv_free(entry);
    }
}

static void send_response(void *arg) {
    struct io_ctx *io = arg;
    io->msg->q_info = ds_queue.q_info[io->worker_id];
    kv_rdma_make_resp(io->req_h, (uint8_t *)io->msg, KV_MSG_SIZE(io->msg));
    kv_mempool_put(io_pool, io);
}

static void forward_cb(void *arg) {
    struct io_ctx *io = arg;
    if (io->need_forward && (io->msg_type == KV_MSG_SET || io->msg_type == KV_MSG_DEL)) {
        del_key(io);
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
    if (!success) {
        io->msg->type = KV_MSG_ERR;
        if (io->need_forward && (io->msg_type == KV_MSG_SET || io->msg_type == KV_MSG_DEL)) {
            del_key(io);
        }
        kv_app_send(io->server_thread, send_response, arg);
        return;
    }
    if (io->need_forward == false) {  // is the last node
        if (io->msg->type == KV_MSG_SET) io->msg->value_len = 0;
        io->msg->type = KV_MSG_OK;
    }
    kv_ring_forward(io->next_node, io->need_forward ? io->req : NULL, forward_cb, io);
}

static void io_start(void *arg) {
    struct io_ctx *io = arg;
    struct worker_t *self = workers + io->worker_id;
    switch (io->msg->type) {
        case KV_MSG_SET:
            if (io->need_forward) put_key(self, KV_MSG_KEY(io->msg), io->msg->key_len);
            io->ds_ctx = kv_data_store_set(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, KV_MSG_VALUE(io->msg),
                                           io->msg->value_len, io_fini, arg);
            break;
        case KV_MSG_GET:
            if (find_key(self, KV_MSG_KEY(io->msg), io->msg->key_len) && io->need_forward) {
                io_fini(true, arg);
            } else {
                io->need_forward = false;
                kv_data_store_get(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, KV_MSG_VALUE(io->msg),
                                  &io->msg->value_len, io_fini, arg);
            }
            break;
        case KV_MSG_DEL:
            assert(io->msg->value_len == 0);
            if (io->need_forward) put_key(self, KV_MSG_KEY(io->msg), io->msg->key_len);
            io->ds_ctx = kv_data_store_delete(&self->data_store, KV_MSG_KEY(io->msg), io->msg->key_len, io_fini, arg);
            break;
        case KV_MSG_TEST:
            io_fini(true, io);
            break;
        default:
            assert(false);
    }
}

static void handler(void *req_h, kv_rdma_mr req, uint32_t req_sz, uint32_t ds_id, void *next, void *arg) {
    uint32_t thread_id = kv_app_get_thread_index();
    struct io_ctx *io = kv_mempool_get(io_pool);
    assert(io);
    io->req_h = req_h;
    io->msg = (struct kv_msg *)kv_rdma_get_req_buf(req);
    io->worker_id = ds_id;
    io->server_thread = thread_id;
    io->req = req;
    io->next_node = next;
    io->need_forward = next != NULL;
    io->msg_type = io->msg->type;
    kv_app_send(io->worker_id, io_start, io);
}

static void ring_init(void *arg) {
    server = kv_ring_init(opt.etcd_ip, opt.etcd_port, opt.thread_num, NULL, NULL);
    io_pool = kv_mempool_create(opt.concurrent_io_num, sizeof(struct io_ctx));
    kv_ring_server_init(opt.local_ip, opt.local_port, opt.ring_num, opt.vid_per_ssd, opt.ssd_num, opt.rpl_num,
                        workers->data_store.bucket_log.log_bucket_num, opt.concurrent_io_num,
                        sizeof(struct kv_msg) + 16 + opt.value_size, handler, NULL, NULL, NULL);
}

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
    self->dirty_keys = NULL;
    kv_storage_init(&self->storage, self - workers);
    uint32_t bucket_num = opt.num_items / KV_ITEM_PER_BUCKET;
    uint64_t value_log_block_num = self->storage.num_blocks * 0.95 - 2 * bucket_num;
    kv_data_store_init(&self->data_store, &self->storage, 0, bucket_num, value_log_block_num, 512, &ds_queue, self - workers,
                       worker_init_done, NULL);
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
    kv_ds_queue_init(&ds_queue, opt.ssd_num);
    kv_app_start(opt.json_config_file, opt.ssd_num + opt.thread_num, task);
    kv_ds_queue_fini(&ds_queue);
    free(workers);
    free(task);
    return 0;
}