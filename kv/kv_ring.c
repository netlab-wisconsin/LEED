#include "kv_ring.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>

#include "kv_app.h"
#include "kv_ds_queue.h"
#include "kv_etcd.h"
#include "kv_memory.h"
#include "kv_msg.h"
#include "pthread.h"
#include "utils/city.h"
#include "utils/uthash.h"

#define CIRCLEQ_FOREACH_SAFE(var, head, field, tvar) \
    for ((var) = CIRCLEQ_FIRST(head); (var) != (void *)(head) && ((tvar) = CIRCLEQ_NEXT(var, field), 1); (var) = (tvar))

struct kv_node {
    struct kv_node_info *info;
    connection_handle conn;
    struct kv_ds_queue ds_queue;
    bool is_connected, is_disconnecting, is_local;
    STAILQ_ENTRY(kv_node) next;
    UT_hash_handle hh;
};
struct vid_entry {
    struct kv_vid *vid;
    struct kv_node *node;
    CIRCLEQ_ENTRY(vid_entry) entry;
};
CIRCLEQ_HEAD(vid_ring, vid_entry);

struct dispatch_ctx {
    kv_rmda_mr req;
    kv_rmda_mr resp;
    void *resp_addr;
    kv_ring_cb cb;
    void *cb_arg;
    uint32_t thread_id;
    struct vid_entry *entry;
    TAILQ_ENTRY(dispatch_ctx) next;
};
TAILQ_HEAD(dispatch_queue, dispatch_ctx);

struct kv_ring {
    kv_rdma_handle h;
    uint32_t thread_id, thread_num;
    char *local_ip;
    char *local_port;
    struct kv_node *nodes;
    struct vid_ring **rings;
    struct dispatch_queue *dqs;
    void **dq_pollers;
    uint32_t vid_num;
    STAILQ_HEAD(, kv_node) conn_q;
    void *conn_q_poller;
    kv_ring_cb ready_cb;
    void *arg;
} g_ring;

static void random_vid(char *vid) {
    for (size_t i = 0; i < KV_VID_LEN; i++) vid[i] = random() & 0xFF;
}
struct vid_src_buf {
    char rdma_ip[16];
    char rdma_port[8];
    uint64_t index;
};
static inline void hash_vid(char *vid, char *ip, char *port, uint32_t index) {
    struct vid_src_buf buf;
    kv_memset(vid, 0, 20);
    kv_memset(&buf, 0, sizeof(buf));
    strcpy(buf.rdma_ip, ip);
    strcpy(buf.rdma_port, port);
    buf.index = index;
    uint128 hash = CityHash128((const char *)&buf, sizeof(buf));
    kv_memcpy(vid, &hash, sizeof(hash));
}
static inline size_t ring_size(struct vid_ring *ring) {
    struct vid_entry *x;
    size_t size = 0;
    CIRCLEQ_FOREACH(x, ring, entry) size++;
    return size;
}

static inline uint32_t get_vid_part(uint8_t *vid, uint32_t vid_num) { return *(uint32_t *)(vid + 12) % vid_num; }
static inline uint64_t get_vid_64(uint8_t *vid) { return *(uint64_t *)(vid + 4); }
static struct vid_entry *find_vid_entry(struct vid_ring *ring, char *vid) {
    if (CIRCLEQ_EMPTY(ring)) return NULL;
    uint64_t base_vid = get_vid_64(CIRCLEQ_LAST(ring)->vid->vid) + 1;
    uint64_t d = get_vid_64(vid) - base_vid;
    struct vid_entry *x;
    CIRCLEQ_FOREACH(x, ring, entry) {
        if (get_vid_64(x->vid->vid) - base_vid >= d) break;
    }
    assert(x != (const void *)(ring));
    return x;
}

static void add_node_to_rings(void *arg) {
    struct kv_node *node = arg;
    struct kv_ring *self = &g_ring;
    struct vid_ring *ring = self->rings[kv_app_get_thread_index() - self->thread_id];
    for (size_t i = 0; i < self->vid_num; i++) {  // add vids to ring
        struct vid_entry *entry = kv_malloc(sizeof(struct vid_entry)), *x;
        *entry = (struct vid_entry){node->info->vids + i, node};
        x = find_vid_entry(ring + i, node->info->vids[i].vid);
        if (x) {
            CIRCLEQ_INSERT_BEFORE(ring + i, x, entry, entry);
        } else {
            CIRCLEQ_INSERT_HEAD(ring + i, entry, entry);
        }
    }
}

static void del_node_from_rings(void *arg) {
    struct kv_node *node = arg;
    struct kv_ring *self = &g_ring;
    struct vid_ring *ring = self->rings[kv_app_get_thread_index() - self->thread_id];
    for (size_t i = 0; i < self->vid_num; i++) {  // remove vids from ring
        struct vid_entry *x = NULL, *tmp;
        CIRCLEQ_FOREACH_SAFE(x, ring + i, entry, tmp) {
            if (x->node == node) CIRCLEQ_REMOVE(ring + i, x, entry);
        }
    }
}
static inline struct vid_entry *find_vid_entry_from_key(char *key, struct vid_ring **p_ring) {
    struct kv_ring *self = &g_ring;
    if (self->rings == NULL) {
        fprintf(stderr, "no available server!\n");
        exit(-1);
    }
    struct vid_ring *ring = self->rings[kv_app_get_thread_index() - self->thread_id];
    uint32_t index = get_vid_part(key, self->vid_num);
    struct vid_entry *entry = find_vid_entry(ring + index, key);
    if (entry == NULL) {
        fprintf(stderr, "no available server for this partition!\n");
        exit(-1);
    }
    if (p_ring) *p_ring = ring + index;
    return entry;
}
static inline struct vid_entry *get_tail(char *key) {
    struct vid_ring *ring;
    struct vid_entry *base_entry = find_vid_entry_from_key(key, &ring), *entry = base_entry;
    for (uint16_t i = 1; i < base_entry->node->info->rpl_num; i++) {
        struct vid_entry *next = CIRCLEQ_LOOP_NEXT(ring, entry, entry);
        if (next == base_entry) break;
        entry = next;
    }
    return entry;
}
connection_handle kv_ring_get_tail(char *key, uint32_t r_num) {
    struct vid_entry *entry = get_tail(key);
    return entry->node->is_local ? NULL : entry->node->conn;
}

static void dispatch_send_cb(connection_handle h, bool success, kv_rmda_mr req, kv_rmda_mr resp, void *cb_arg) {
    struct dispatch_ctx *ctx = cb_arg;
    struct kv_msg *msg = ctx->resp_addr;
    ctx->entry->node->ds_queue.io_cnt[ctx->entry->vid->ds_id]--;
    ctx->entry->node->ds_queue.q_info[ctx->entry->vid->ds_id] = msg->q_info;
    if (!success) msg->type = KV_MSG_ERR;
    if (ctx->cb) kv_app_send(ctx->thread_id, ctx->cb, ctx->cb_arg);
    kv_free(ctx);
}

#if 1
static bool try_send_req(struct dispatch_ctx *ctx) {
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(ctx->req);
    struct vid_ring *ring;
    struct vid_entry *entry = find_vid_entry_from_key(KV_MSG_KEY(msg), &ring);
    if (msg->type == KV_MSG_GET) {
        uint32_t rpl_num = entry->node->info->rpl_num;
        struct kv_ds_q_info *q_info = kv_calloc(rpl_num, sizeof(struct kv_ds_q_info));
        uint32_t *io_cnt = kv_calloc(rpl_num, sizeof(uint32_t));
        struct vid_entry *x = entry, **entries = kv_calloc(rpl_num, sizeof(struct vid_entry *));
        uint32_t i = 0;
        while (i < rpl_num) {
            q_info[i] = x->node->ds_queue.q_info[x->vid->ds_id];
            io_cnt[i] = x->node->ds_queue.io_cnt[x->vid->ds_id];
            entries[i] = x;
            ++i;
            if ((x = CIRCLEQ_LOOP_NEXT(ring, entry, entry)) == entry) break;
        }
        struct kv_ds_q_info *y = kv_ds_queue_find(q_info, io_cnt, i, kv_ds_op_cost(KV_DS_GET));
        if (y) {
            x = entries[y - q_info];
            x->node->ds_queue.io_cnt[x->vid->ds_id]++;
            x->node->ds_queue.q_info[x->vid->ds_id] = *y;
            msg->ds_id = x->vid->ds_id;
            ctx->entry = x;
            kv_rmda_send_req(x->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->resp, ctx->resp_addr, dispatch_send_cb, ctx);
        }
        kv_free(entries);
        kv_free(io_cnt);
        kv_free(q_info);
        return y != NULL;
    } else {
        uint32_t cost = kv_ds_op_cost(msg->type == KV_MSG_SET ? KV_DS_SET : KV_DS_DEL);
        struct kv_ds_q_info q_info = entry->node->ds_queue.q_info[entry->vid->ds_id];
        uint32_t io_cnt = entry->node->ds_queue.io_cnt[entry->vid->ds_id];
        if (kv_ds_queue_find(&q_info, &io_cnt, 1, cost)) {
            entry->node->ds_queue.io_cnt[entry->vid->ds_id]++;
            entry->node->ds_queue.q_info[entry->vid->ds_id] = q_info;
            msg->ds_id = entry->vid->ds_id;
            ctx->entry = entry;
            kv_rmda_send_req(entry->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->resp, ctx->resp_addr, dispatch_send_cb, ctx);
            return true;
        }
        return false;
    }
}
#else
static bool try_send_req(struct dispatch_ctx *ctx) {
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(ctx->req);
    uint8_t *key = KV_MSG_KEY(msg);
    struct vid_entry *entry = msg->type == KV_MSG_GET ? get_tail(key) : find_vid_entry_from_key(key, NULL);
    ctx->entry = entry;
    entry->node->ds_queue.io_cnt[entry->vid->ds_id]++;
    msg->ds_id = entry->vid->ds_id;
    kv_rmda_send_req(entry->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->resp, ctx->resp_addr, dispatch_send_cb, ctx);
    return true;
}
#endif
#define TAILQ_FOREACH_SAFE(var, head, field, tvar) \
    for ((var) = TAILQ_FIRST((head)); (var) && ((tvar) = TAILQ_NEXT((var), field), 1); (var) = (tvar))
static int dispatch_dequeue(void *arg) {
    struct kv_ring *self = arg;
    struct dispatch_queue *dp = &self->dqs[kv_app_get_thread_index() - self->thread_id];
    struct dispatch_ctx *x, *tmp;
    TAILQ_FOREACH_SAFE(x, dp, next, tmp) {
        if (try_send_req(x)) TAILQ_REMOVE(dp, x, next);
    }
    return 0;
}
static void dispatch(void *arg) {
    struct kv_ring *self = &g_ring;
    struct dispatch_ctx *ctx = arg;
    if (!try_send_req(ctx)) {
        struct dispatch_queue *dp = &self->dqs[kv_app_get_thread_index() - self->thread_id];
        TAILQ_INSERT_TAIL(dp, ctx, next);
        // kv_app_send(self->thread_id + random() % self->thread_num, dispatch, ctx);
    }
}
void kv_ring_dispatch(kv_rmda_mr req, kv_rmda_mr resp, void *resp_addr, kv_ring_cb cb, void *cb_arg) {
    struct kv_ring *self = &g_ring;
    struct dispatch_ctx *ctx = kv_malloc(sizeof(*ctx));
    *ctx = (struct dispatch_ctx){req, resp, resp_addr, cb, cb_arg, kv_app_get_thread_index()};
    ((struct kv_msg *)kv_rdma_get_req_buf(ctx->req))->hop = 1;
    if (ctx->thread_id >= self->thread_id && ctx->thread_id < self->thread_id + self->thread_num) {
        dispatch(ctx);
    } else {
        kv_app_send(self->thread_id + random() % self->thread_num, dispatch, ctx);
    }
};

void kv_ring_forward(char *key, uint32_t hop, uint32_t r_num, connection_handle *h, uint16_t *ds_id) {
    if (hop >= r_num) {
        *h = NULL;
        return;
    }
    struct vid_ring *ring;
    struct vid_entry *base_entry = find_vid_entry_from_key(key, &ring), *entry = base_entry;
    for (uint16_t i = 0; i < hop; i++) {
        entry = CIRCLEQ_LOOP_NEXT(ring, entry, entry);
        if (entry == base_entry) {
            *h = NULL;
            return;
        }
    }
    assert(!entry->node->is_local);
    *h = entry->node->conn;
    *ds_id = entry->vid->ds_id;
}

static void rdma_disconnect_cb(void *arg) {
    struct kv_ring *self = &g_ring;
    struct kv_node *node = arg;
    if (node->is_disconnecting) {
        printf("client: disconnected to node %s:%s.\n", node->info->rdma_ip, node->info->rdma_port);
        kv_ds_queue_fini(&node->ds_queue);
        free(node->info);  // info is allocated by libkv_etcd
        kv_free(node);
    } else {
        node->is_connected = false;
        STAILQ_INSERT_TAIL(&self->conn_q, node, next);
    }
    for (size_t i = 0; i < self->thread_num; i++) kv_app_send(self->thread_id + i, del_node_from_rings, node);
}
static void rdma_connect_cb(connection_handle h, void *arg) {
    struct kv_ring *self = &g_ring;
    struct kv_node *node = arg;
    assert(!node->is_local);
    if (!h) fprintf(stderr, "can't connect to node %s:%s, retrying ...\n", node->info->rdma_ip, node->info->rdma_port);
    if (node->is_disconnecting || !h) {
        // disconnect or retry
        STAILQ_INSERT_TAIL(&self->conn_q, node, next);
        return;
    }
    printf("client: connected to node %s:%s.\n", node->info->rdma_ip, node->info->rdma_port);
    node->is_connected = true;
    node->conn = h;
    for (size_t i = 0; i < self->thread_num; i++) kv_app_send(self->thread_id + i, add_node_to_rings, node);
}

static int kv_conn_q_poller(void *arg) {
    struct kv_ring *self = arg;
    while (!STAILQ_EMPTY(&self->conn_q)) {
        struct kv_node *node = STAILQ_FIRST(&self->conn_q);
        STAILQ_REMOVE_HEAD(&self->conn_q, next);
        assert(!node->is_local);
        if (node->is_disconnecting) {
            if (node->is_connected)
                kv_rdma_disconnect(node->conn);
            else
                rdma_disconnect_cb(node);
        } else {
            assert(!node->is_connected);
            kv_rdma_connect(self->h, node->info->rdma_ip, node->info->rdma_port, rdma_connect_cb, node, rdma_disconnect_cb,
                            node);
        }
    }
    if (self->ready_cb) {
        for (struct kv_node *node = self->nodes; node != NULL; node = node->hh.next)
            if (!node->is_local && !node->is_disconnecting && !node->is_connected) return 0;
        self->ready_cb(self->arg);
        self->ready_cb = NULL;  // call ready_cb only once
    }
    return 0;
}
static void ring_init(uint32_t vid_num);
static void node_handler(void *arg) {
    struct kv_node_info *info = arg;
    struct kv_ring *self = &g_ring;
    ring_init(info->vid_num);
    if (info->msg_type == KV_NODE_INFO_DELETE) {
        struct kv_node *node = NULL;
        HASH_FIND(hh, self->nodes, info->rdma_ip, 24, node);
        assert(node);
        HASH_DEL(self->nodes, node);
        if (node->is_local) {
            fprintf(stderr, "kv_etcd keepalive timeout!\n");
            exit(-1);
        } else {
            node->is_disconnecting = true;
            if (node->is_connected) STAILQ_INSERT_TAIL(&self->conn_q, node, next);
        }
    } else {
        struct kv_node *node = kv_malloc(sizeof(struct kv_node));
        node->is_local = self->local_ip && !strcmp(self->local_ip, info->rdma_ip) && !strcmp(self->local_port, info->rdma_port);
        node->is_disconnecting = false;
        node->is_connected = false;
        node->info = info;
        kv_ds_queue_init(&node->ds_queue, info->ds_num);
        for (uint32_t i = 0; i < info->ds_num; ++i) {
            node->ds_queue.q_info[i] = (struct kv_ds_q_info){0, 0};
            node->ds_queue.io_cnt[i] = 0;
        }
        // node may already exist?
        HASH_ADD(hh, self->nodes, info->rdma_ip, 24, node);  // ip & port as key
        if (node->is_local) {
            for (size_t i = 0; i < self->thread_num; i++) kv_app_send(self->thread_id + i, add_node_to_rings, node);
        } else {
            STAILQ_INSERT_TAIL(&self->conn_q, node, next);
        }
    }
}

static void _node_handler(struct kv_node_info *info) {
    struct kv_ring *self = &g_ring;
    if (info->msg_type == KV_NODE_INFO_READ) {
        assert(self->thread_id == kv_app_get_thread_index());
        node_handler(info);
    } else {
        kv_app_send_without_token(self->thread_id, node_handler, info);
    }
}

static int kv_etcd_poller(void *arg) {
    kvEtcdKeepAlive();
    return 0;
}

static void ring_init(uint32_t vid_num) {
    struct kv_ring *self = &g_ring;
    if (self->rings == NULL) {
        self->vid_num = vid_num;
        self->rings = kv_calloc(self->thread_num, sizeof(struct vid_ring *));
        for (size_t i = 0; i < self->thread_num; i++) {
            self->rings[i] = kv_calloc(self->vid_num, sizeof(struct vid_ring));
            for (size_t j = 0; j < vid_num; j++) CIRCLEQ_INIT(self->rings[i] + j);
        }
    }
    if (self->dqs == NULL) {
        self->dqs = kv_calloc(self->thread_num, sizeof(struct dispatch_queue));
        self->dq_pollers = kv_calloc(self->thread_num, sizeof(void *));
        for (size_t i = 0; i < self->thread_num; i++) {
            TAILQ_INIT(self->dqs + i);
            kv_app_poller_register_on(self->thread_id + i, dispatch_dequeue, self, 200, &self->dq_pollers[i]);
        }
    }
}

kv_rdma_handle kv_ring_init(char *etcd_ip, char *etcd_port, uint32_t thread_num, kv_ring_cb ready_cb, void *arg) {
    struct kv_ring *self = &g_ring;
    self->ready_cb = ready_cb;
    self->arg = arg;
    self->nodes = NULL;
    self->rings = NULL;
    self->dqs = NULL;
    STAILQ_INIT(&self->conn_q);
    self->thread_id = kv_app_get_thread_index();
    self->thread_num = thread_num;
    kv_rdma_init(&self->h, thread_num);
    kvEtcdInit(etcd_ip, etcd_port, _node_handler, NULL);
    self->conn_q_poller = kv_app_poller_register(kv_conn_q_poller, self, 1000000);
    return self->h;
}

struct vid_ring_stat {
    uint32_t index;
    size_t size;
};
static int vid_ring_stat_cmp(const void *_a, const void *_b) {
    const struct vid_ring_stat *a = _a, *b = _b;
    if (a->size == b->size) return 0;
    return a->size < b->size ? -1 : 1;
}

void kv_ring_server_init(char *local_ip, char *local_port, uint32_t vid_num, uint32_t vid_per_ssd, uint32_t ds_num,
                         uint32_t rpl_num, uint32_t con_req_num, uint32_t max_msg_sz, kv_rdma_req_handler handler, void *arg,
                         kv_rdma_server_init_cb cb, void *cb_arg) {
    assert(ds_num != 0);
    assert(vid_per_ssd * ds_num <= vid_num);
    assert(local_ip && local_port);
    struct kv_ring *self = &g_ring;
    kv_rdma_listen(self->h, local_ip, local_port, con_req_num, max_msg_sz, handler, arg, cb, cb_arg);
    self->local_ip = local_ip;
    self->local_port = local_port;
    ring_init(vid_num);
    struct kv_node_info *info = kv_node_info_alloc(local_ip, local_port, vid_num);
    info->rpl_num = rpl_num;
    info->ds_num = ds_num;
    struct vid_ring_stat *stats = kv_calloc(vid_num, sizeof(struct vid_ring_stat));
    for (size_t i = 0; i < vid_num; i++) stats[i] = (struct vid_ring_stat){i, ring_size(self->rings[0] + i)};
    qsort(stats, vid_num, sizeof(struct vid_ring_stat), vid_ring_stat_cmp);
    uint32_t ds_id = 0;
    for (size_t i = 0; i < vid_per_ssd * ds_num; i++) {
        info->vids[stats[i].index].ds_id = ds_id;
        // random_vid(info->vids[stats[i].index].vid);
        hash_vid(info->vids[stats[i].index].vid, local_ip, local_port, i);
        ds_id = (ds_id + 1) % ds_num;
    }
    kv_free(stats);
    kvEtcdCreateNode(info, 1);
    free(info);
    kv_app_poller_register(kv_etcd_poller, NULL, 300000);
}

void kv_ring_fini(kv_rdma_fini_cb cb, void *cb_arg) {
    struct kv_ring *self = &g_ring;
    kvEtcdFini();
    // TODO: diconnect all nodes
    kv_app_poller_unregister(&self->conn_q_poller);
    kv_rdma_fini(self->h, cb, cb_arg);
    if (self->rings) {
        for (size_t i = 0; i < self->thread_num; i++) kv_free(self->rings[i]);
        kv_free(self->rings);
    }
    if (self->dqs) {
        for (size_t i = 0; i < self->thread_num; i++) kv_app_poller_unregister(&self->dq_pollers[i]);
        kv_free(self->dqs);
        kv_free(self->dq_pollers);
    }
}