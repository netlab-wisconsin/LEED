#include "kv_ring.h"

#include <assert.h>
#include <stdatomic.h>
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

// etcd values
struct kv_etcd_node {
    uint32_t ring_num;
    uint16_t rpl_num;
    uint16_t ds_num;
} __attribute__((packed));

enum { VID_COPYING,
       VID_RUNNING,
       VID_LEAVING,
       VID_DELETING };
struct kv_etcd_vid {
#define KV_VID_EMPTY UINT32_MAX
#define KV_VID_LEN (20U)
    uint16_t state;
    uint16_t ds_id;
    uint8_t vid[KV_VID_LEN];
} __attribute__((packed));

//---- rings ----

struct kv_node {
#define KV_MAX_NODEID_LEN (24U)
    char node_id[KV_MAX_NODEID_LEN];
    struct kv_etcd_node info;
    connection_handle conn;
    struct kv_ds_queue ds_queue;
    bool is_connected, is_disconnecting, is_local;
    STAILQ_ENTRY(kv_node)
    next;
    UT_hash_handle hh;
};
struct vid_entry {
    struct kv_etcd_vid vid;
    struct kv_node *node;
    CIRCLEQ_ENTRY(vid_entry)
    entry;
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
    TAILQ_ENTRY(dispatch_ctx)
    next;
};
TAILQ_HEAD(dispatch_queue, dispatch_ctx);

struct kv_ring {
    kv_rdma_handle h;
    uint32_t thread_id, thread_num;
    char local_id[KV_MAX_NODEID_LEN];
    struct kv_node *nodes;
    struct vid_ring **rings;
    struct dispatch_queue *dqs;
    void **dq_pollers;
    uint32_t ring_num;
    STAILQ_HEAD(, kv_node)
    conn_q;
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
static inline size_t ring_size(struct vid_ring *ring) {  // TODO: only count active vids
    struct vid_entry *x;
    size_t size = 0;
    CIRCLEQ_FOREACH(x, ring, entry)
    size++;
    return size;
}

// key:
// |-- 4 bytes --|-- 8 bytes --|-- 4 bytes --|-- 4 bytes --|
// |bucket index |  64bit vid  |   ring id   |   reserved  |
static inline uint32_t get_vid_part(uint8_t *vid, uint32_t ring_num) { return *(uint32_t *)(vid + 12) % ring_num; }
static inline uint64_t get_vid_64(uint8_t *vid) { return *(uint64_t *)(vid + 4); }
static struct vid_entry *find_vid_entry(struct vid_ring *ring, char *vid) {
    if (CIRCLEQ_EMPTY(ring)) return NULL;
    uint64_t base_vid = get_vid_64(CIRCLEQ_LAST(ring)->vid.vid) + 1;
    uint64_t d = get_vid_64(vid) - base_vid;
    struct vid_entry *x;
    CIRCLEQ_FOREACH(x, ring, entry) {
        if (get_vid_64(x->vid.vid) - base_vid >= d) break;
    }
    assert(x != (const void *)(ring));
    return x;
}

static inline struct vid_entry *find_vid_entry_from_key(char *key, struct vid_ring **p_ring) {
    struct kv_ring *self = &g_ring;
    if (self->rings == NULL) {
        fprintf(stderr, "no available server!\n");
        exit(-1);
    }
    struct vid_ring *ring = self->rings[kv_app_get_thread_index() - self->thread_id];
    uint32_t index = get_vid_part(key, self->ring_num);
    struct vid_entry *entry = find_vid_entry(ring + index, key);
    if (entry == NULL) {
        fprintf(stderr, "no available server for this partition!\n");
        exit(-1);
    }
    if (p_ring) *p_ring = ring + index;
    return entry;
}
static inline struct vid_entry *get_node(char *key, uint32_t n) {
    struct vid_ring *ring;
    struct vid_entry *base_entry = find_vid_entry_from_key(key, &ring), *entry = base_entry;
    for (uint16_t i = 1; i < n; i++) {
        struct vid_entry *next = CIRCLEQ_LOOP_NEXT(ring, entry, entry);
        if (next == base_entry) break;
        entry = next;
    }
    return entry;
}
connection_handle kv_ring_get_tail(char *key, uint32_t r_num) {
    struct vid_entry *entry = get_node(key, r_num);
    return entry->node->is_local ? NULL : entry->node->conn;
}

static void dispatch_send_cb(connection_handle h, bool success, kv_rmda_mr req, kv_rmda_mr resp, void *cb_arg) {
    struct dispatch_ctx *ctx = cb_arg;
    struct kv_msg *msg = ctx->resp_addr;
    ctx->entry->node->ds_queue.io_cnt[ctx->entry->vid.ds_id]--;
    ctx->entry->node->ds_queue.q_info[ctx->entry->vid.ds_id] = msg->q_info;
    if (!success) msg->type = KV_MSG_ERR;
    if (ctx->cb) kv_app_send(ctx->thread_id, ctx->cb, ctx->cb_arg);
    kv_free(ctx);
}
#define DISPATCH_TYPE 0
#if DISPATCH_TYPE == 0
static bool try_send_req(struct dispatch_ctx *ctx) {
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(ctx->req);
    struct vid_ring *ring;
    struct vid_entry *entry = find_vid_entry_from_key(KV_MSG_KEY(msg), &ring);
    if (msg->type == KV_MSG_GET) {
        uint32_t rpl_num = entry->node->info.rpl_num;
        struct kv_ds_q_info *q_info = kv_calloc(rpl_num, sizeof(struct kv_ds_q_info));
        uint32_t *io_cnt = kv_calloc(rpl_num, sizeof(uint32_t));
        struct vid_entry *x = entry, **entries = kv_calloc(rpl_num, sizeof(struct vid_entry *));
        uint32_t i = 0;
        while (i < rpl_num) {
            q_info[i] = x->node->ds_queue.q_info[x->vid.ds_id];
            io_cnt[i] = x->node->ds_queue.io_cnt[x->vid.ds_id];
            entries[i] = x;
            ++i;
            if ((x = CIRCLEQ_LOOP_NEXT(ring, entry, entry)) == entry) break;
        }
        struct kv_ds_q_info *y = kv_ds_queue_find(q_info, io_cnt, i, kv_ds_op_cost(KV_DS_GET));
        if (y) {
            x = entries[y - q_info];
            x->node->ds_queue.io_cnt[x->vid.ds_id]++;
            x->node->ds_queue.q_info[x->vid.ds_id] = *y;
            msg->ds_id = x->vid.ds_id;
            ctx->entry = x;
            kv_rmda_send_req(x->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->resp, ctx->resp_addr, dispatch_send_cb, ctx);
        }
        kv_free(entries);
        kv_free(io_cnt);
        kv_free(q_info);
        return y != NULL;
    } else {
        uint32_t cost = kv_ds_op_cost(msg->type == KV_MSG_SET ? KV_DS_SET : KV_DS_DEL);
        struct kv_ds_q_info q_info = entry->node->ds_queue.q_info[entry->vid.ds_id];
        uint32_t io_cnt = entry->node->ds_queue.io_cnt[entry->vid.ds_id];
        if (kv_ds_queue_find(&q_info, &io_cnt, 1, cost)) {
            entry->node->ds_queue.io_cnt[entry->vid.ds_id]++;
            entry->node->ds_queue.q_info[entry->vid.ds_id] = q_info;
            msg->ds_id = entry->vid.ds_id;
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
    struct vid_entry *entry = find_vid_entry_from_key(key, NULL);
    if (msg->type == KV_MSG_GET) {
#if DISPATCH_TYPE == 1
        entry = get_node(key, entry->node->info->rpl_num);  // forward to tail
#else
        entry = get_node(key, (random() % entry->node->info->rpl_num) + 1);  // random
#endif
    }
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
    *ds_id = entry->vid.ds_id;
}

static void ring_init(uint32_t ring_num) {
    struct kv_ring *self = &g_ring;
    if (self->rings == NULL) {
        self->ring_num = ring_num;
        self->rings = kv_calloc(self->thread_num, sizeof(struct vid_ring *));
        for (size_t i = 0; i < self->thread_num; i++) {
            self->rings[i] = kv_calloc(self->ring_num, sizeof(struct vid_ring));
            for (size_t j = 0; j < ring_num; j++) CIRCLEQ_INIT(self->rings[i] + j);
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
};

static void on_vid_state_change(struct vid_entry *entry) {
    // struct kv_ring *self = &g_ring;
    switch (entry->vid.state) {
        case VID_COPYING:
            // TAIL starts to copy data
            break;
        case VID_RUNNING:
            break;
        case VID_LEAVING:
            // TAIL starts to copy data
            break;
        case VID_DELETING:
            break;
    }
}

struct ring_change_ctx {
    struct kv_etcd_vid vid;
    char node_id[20];
    uint32_t ring_id;
    struct kv_node *node;
    _Atomic uint32_t ref_cnt;
};

static void update_ring(void *arg) {
    struct kv_ring *self = &g_ring;
    struct ring_change_ctx *ctx = arg;
    struct vid_ring *ring = self->rings[kv_app_get_thread_index() - self->thread_id];
    struct vid_entry *entry = find_vid_entry(ring + ctx->ring_id, ctx->vid.vid);
    if (entry == NULL || kv_memcmp8(entry->vid.vid, ctx->vid.vid, KV_VID_LEN)) {
        if (ctx->vid.state != VID_DELETING) {
            struct vid_entry *x = kv_malloc(sizeof(*x));
            *x = (struct vid_entry){.vid = ctx->vid, .node = ctx->node};
            if (entry) {
                CIRCLEQ_INSERT_BEFORE(ring + ctx->ring_id, entry, x, entry);
            } else {
                CIRCLEQ_INSERT_HEAD(ring + ctx->ring_id, x, entry);
            }
        }
    } else {
        if (ctx->vid.state != VID_DELETING) {
            CIRCLEQ_REMOVE(ring + ctx->ring_id, entry, entry);
            kv_free(entry);
        } else {
            entry->vid = ctx->vid;
        }
    }
    if (--ctx->ref_cnt == 0) kv_free(ctx);
};

static void on_ring_change(void *arg) {
    struct kv_ring *self = &g_ring;
    struct ring_change_ctx *ctx = arg;
    HASH_FIND_STR(self->nodes, ctx->node_id, ctx->node);
    ctx->ref_cnt = self->thread_num;
    update_ring(ctx);
    // on_vid_state_change
    for (size_t i = 1; i < self->thread_num; i++) kv_app_send(self->thread_id + i, update_ring, ctx);
}

static void ring_handler(enum kv_etcd_msg_type msg, uint32_t ring_id, const char *node_id, const void *value, uint32_t val_len) {
    struct kv_ring *self = &g_ring;
    struct ring_change_ctx *ctx = kv_malloc(sizeof(*ctx));
    assert(val_len == sizeof(ctx->vid));
    kv_memcpy(&ctx->vid, value, val_len);
    strcpy(ctx->node_id, node_id);
    ctx->ring_id = ring_id;
    if (msg == KV_ETCD_MSG_DEL) ctx->vid.state = VID_DELETING;
    kv_app_send_without_token(self->thread_id, on_ring_change, ctx);
}

static void rdma_disconnect_cb(void *arg) {
    struct kv_ring *self = &g_ring;
    struct kv_node *node = arg;
    if (node->is_disconnecting) {
        printf("client: disconnected to node %s.\n", node->node_id);
        kv_ds_queue_fini(&node->ds_queue);
        kv_free(node);
    } else {
        node->is_connected = false;
        STAILQ_INSERT_TAIL(&self->conn_q, node, next);
    }
    // for (size_t i = 0; i < self->thread_num; i++) kv_app_send(self->thread_id + i, del_node_from_rings, node);
}
static void rdma_connect_cb(connection_handle h, void *arg) {
    struct kv_ring *self = &g_ring;
    struct kv_node *node = arg;
    assert(!node->is_local);
    if (!h) fprintf(stderr, "can't connect to node %s, retrying ...\n", node->node_id);
    if (node->is_disconnecting || !h) {
        // disconnect or retry
        STAILQ_INSERT_TAIL(&self->conn_q, node, next);
        return;
    }
    printf("client: connected to node %s.\n", node->node_id);
    node->is_connected = true;
    node->conn = h;
    // for (size_t i = 0; i < self->thread_num; i++) kv_app_send(self->thread_id + i, add_node_to_rings, node);
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
            char ip_port[KV_MAX_NODEID_LEN];
            strcpy(ip_port, node->node_id);
            kv_rdma_connect(self->h, strtok(ip_port, ":"), strtok(NULL, ":"), rdma_connect_cb, node, rdma_disconnect_cb,
                            node);
        }
    }
    if (self->ready_cb) {
        for (struct kv_node *node = self->nodes; node != NULL; node = node->hh.next)
            if (!node->is_local && !node->is_disconnecting && !node->is_connected) return 0;
        self->ready_cb(self->arg);
        self->ready_cb = NULL;  // only call ready_cb once
    }
    return 0;
}

static void on_node_del(void *arg) {
    char *node_id = arg;
    struct kv_ring *self = &g_ring;
    struct kv_node *node = NULL;
    HASH_FIND_STR(self->nodes, node_id, node);
    if (node == NULL) goto finish;
    HASH_DEL(self->nodes, node);
    if (node->is_local) {
        fprintf(stderr, "kv_etcd keepalive timeout!\n");
        exit(-1);
    }
    node->is_disconnecting = true;
    if (node->is_connected) STAILQ_INSERT_TAIL(&self->conn_q, node, next);
finish:
    kv_free(node_id);
}

static void on_node_put(void *arg) {
    struct kv_node *node = arg, *tmp = NULL;
    struct kv_ring *self = &g_ring;
    ring_init(node->info.ring_num);
    HASH_FIND_STR(self->nodes, node->node_id, tmp);
    if (tmp != NULL) {  // this node already exist
        kv_free(node);
        return;
    }
    node->is_local = !strcmp(self->local_id, node->node_id);
    node->is_disconnecting = false;
    node->is_connected = false;
    kv_ds_queue_init(&node->ds_queue, node->info.ds_num);
    for (uint32_t i = 0; i < node->info.ds_num; ++i) {
        node->ds_queue.q_info[i] = (struct kv_ds_q_info){0, 0};
        node->ds_queue.io_cnt[i] = 0;
    }
    HASH_ADD_STR(self->nodes, node_id, node);
    if (!node->is_local) STAILQ_INSERT_TAIL(&self->conn_q, node, next);
}

static void node_handler(enum kv_etcd_msg_type msg, const char *node_id, const void *val, uint32_t val_len) {
    struct kv_ring *self = &g_ring;
    if (msg == KV_ETCD_MSG_PUT) {
        struct kv_node *node = kv_malloc(sizeof(struct kv_node));
        strcpy(node->node_id, node_id);
        assert(val_len == sizeof(struct kv_etcd_node));
        kv_memcpy(&node->info, val, val_len);
        kv_app_send_without_token(self->thread_id, on_node_put, node);
    } else if (msg == KV_ETCD_MSG_DEL) {
        char *nodeID = kv_malloc(KV_MAX_NODEID_LEN);
        strcpy(nodeID, node_id);
        kv_app_send_without_token(self->thread_id, on_node_del, nodeID);
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
    kvEtcdInit(etcd_ip, etcd_port, node_handler, ring_handler);
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

void kv_ring_server_init(char *local_ip, char *local_port, uint32_t ring_num, uint32_t vid_per_ssd, uint32_t ds_num,
                         uint32_t rpl_num, uint32_t con_req_num, uint32_t max_msg_sz, kv_rdma_req_handler handler, void *arg,
                         kv_rdma_server_init_cb cb, void *cb_arg) {
    assert(ds_num != 0);
    assert(vid_per_ssd * ds_num <= ring_num);
    assert(local_ip && local_port);
    struct kv_ring *self = &g_ring;
    kv_rdma_listen(self->h, local_ip, local_port, con_req_num, max_msg_sz, handler, arg, cb, cb_arg);

    sprintf(self->local_id, "%s:%s", local_ip, local_port);
    ring_init(ring_num);
    struct kv_etcd_node etcd_node = {.ds_num = ds_num, .ring_num = ring_num, .rpl_num = rpl_num};
    kvEtcdNodeReg(self->local_id, &etcd_node, sizeof(etcd_node), 1);

    struct vid_ring_stat stats[ring_num];
    for (size_t i = 0; i < ring_num; i++) stats[i] = (struct vid_ring_stat){i, ring_size(self->rings[0] + i)};
    qsort(stats, ring_num, sizeof(struct vid_ring_stat), vid_ring_stat_cmp);
    uint32_t ds_id = 0;
    for (size_t i = 0; i < vid_per_ssd * ds_num; i++) {
        struct kv_etcd_vid vid = {.state = VID_COPYING, .ds_id = ds_id};
        // random_vid(vid.vid);
        hash_vid(vid.vid, local_ip, local_port, i);
        kvEtcdVidPut(self->local_id, stats[i].index, &vid, sizeof(vid));
        ds_id = (ds_id + 1) % ds_num;
    }
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