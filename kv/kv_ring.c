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
       VID_LEAVING };
struct kv_etcd_vid {
#define KV_VID_LEN (20U)
    // uint16_t state;
    uint16_t ds_id;
    uint8_t vid[KV_VID_LEN];
} __attribute__((packed));

//---- rings ----
#define KV_MAX_NODEID_LEN (24U)
struct ring_change_ctx {
    struct kv_etcd_vid vid;
    char node_id[KV_MAX_NODEID_LEN];
    uint32_t ring_id, state, msg_type;
    struct kv_node *node;
    uint32_t cnt;  // only used in master thread
    STAILQ_ENTRY(ring_change_ctx)
    next;
};

struct kv_node {
    char node_id[KV_MAX_NODEID_LEN];
    struct kv_etcd_node info;
    connection_handle conn;
    struct kv_ds_queue ds_queue;
    bool is_connected, is_disconnecting, is_local, has_info;
    STAILQ_ENTRY(kv_node)
    next;
    STAILQ_HEAD(, ring_change_ctx)
    ring_updates;
    UT_hash_handle hh;
    _Atomic uint32_t req_cnt;
};
STAILQ_HEAD(kv_nodes_head, kv_node);
struct vid_entry {
    struct kv_etcd_vid vid;
    struct kv_node *node;
    uint32_t state;
    uint32_t cp_cnt, rm_cnt;  // only valid in the master thread
    CIRCLEQ_ENTRY(vid_entry)
    entry;
};

CIRCLEQ_HEAD(vid_ring, vid_entry);

struct dispatch_ctx {
    kv_rdma_mr req;
    kv_rdma_mr resp;
    void *resp_addr;
    kv_ring_cb cb;
    void *cb_arg;
    uint32_t thread_id;
    uint32_t retry_num, next_retry;
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
    struct kv_nodes_head conn_q;
    void *conn_q_poller;
    kv_ring_cb ready_cb;
    void *arg;
    kv_ring_req_handler req_handler;
    uint64_t node_lease, vid_lease;
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

static struct vid_entry *find_vid_by_node(struct vid_ring *ring, struct kv_node *node) {
    if (CIRCLEQ_EMPTY(ring)) return NULL;
    struct vid_entry *x;
    CIRCLEQ_FOREACH(x, ring, entry) {
        assert(x->node);
        if (x->node == node)
            return x;
    }
    return NULL;
}

struct vnode_chain {
    struct vid_ring *ring;
    struct vid_entry *base;  // may be a invalid vnode
    uint32_t rpl_num;
};

static struct vid_entry *get_vnode_from_chain(struct vnode_chain *chain, int32_t index, struct vid_entry *base, bool pre_copy) {
    // index: -1 for tail, 0 for head, chain_len for pre_coping vnode.
    // atmost one invalid vnode exists in a ring(chain).
    struct vid_entry *invalid = NULL, *tail = NULL, *x = base && !pre_copy ? base : chain->base;
    int32_t i = 0;
    while (i < (int32_t)chain->rpl_num) {
        if (x->state == VID_RUNNING) {
            if (i == index) return x;
            tail = x;
            ++i;
        } else {
            if (invalid != NULL) {
                fprintf(stderr, "two failed vnodes in a hash ring!\n");
                exit(-1);
            }
            invalid = x;
        }
        if ((x = CIRCLEQ_LOOP_NEXT(chain->ring, x, entry)) == chain->base) break;
    }
    if (index == -1) return tail;
    if (pre_copy && index == i && invalid != NULL) {
        if (invalid->state == VID_LEAVING && x != chain->base)
            return x;  // pre_copy to the next vnode
        if (invalid->state == VID_COPYING)
            return invalid;  // pre_copy to new vnode.
    }
    return NULL;
}
static inline struct vid_entry *get_vnode(struct vnode_chain *chain, int32_t index) {
    return get_vnode_from_chain(chain, index, NULL, false);
}
static inline struct vid_entry *get_vnode_next(struct vnode_chain *chain, struct vid_entry *base) {
    return get_vnode_from_chain(chain, 1, base, false);
}

static bool get_chain(struct vnode_chain *chain, char *key) {
    assert(chain);
    struct kv_ring *self = &g_ring;
    if (self->rings == NULL) return false;
    struct vid_ring *ring = self->rings[kv_app_get_thread_index() - self->thread_id];
    chain->ring = ring + get_vid_part(key, self->ring_num);
    chain->base = find_vid_entry(chain->ring, key);
    if (chain->base == NULL) return false;
    chain->rpl_num = 1;
    // get real rpl_num from head
    chain->rpl_num = get_vnode(chain, 0)->node->info.rpl_num;
    return true;
}

static void dispatch_send_cb(connection_handle h, bool success, kv_rdma_mr req, kv_rdma_mr resp, void *cb_arg) {
    struct kv_ring *self = &g_ring;
    struct dispatch_ctx *ctx = cb_arg;
    struct kv_msg *msg = ctx->resp_addr;
    ctx->entry->node->ds_queue.io_cnt[ctx->entry->vid.ds_id]--;
    ctx->entry->node->ds_queue.q_info[ctx->entry->vid.ds_id] = msg->q_info;
    ctx->entry->node->req_cnt--;
    if (success && msg->type == KV_MSG_OUTDATED) {
        struct dispatch_queue *dp = &self->dqs[kv_app_get_thread_index() - self->thread_id];
        TAILQ_INSERT_TAIL(dp, ctx, next);
        return;
    }
    if (!success) msg->type = KV_MSG_ERR;
    if (ctx->cb) kv_app_send(ctx->thread_id, ctx->cb, ctx->cb_arg);
    kv_free(ctx);
}
#define DISPATCH_TYPE 0
#if DISPATCH_TYPE == 0
static bool try_send_req(struct dispatch_ctx *ctx) {
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(ctx->req);
    struct vnode_chain chain;
    if (get_chain(&chain, KV_MSG_KEY(msg)) == false) return false;
    msg->hop = 1;
    if (msg->type == KV_MSG_GET) {
        struct kv_ds_q_info q_info[chain.rpl_num];
        uint32_t io_cnt[chain.rpl_num];
        uint32_t i = 0;
        for (struct vid_entry *x = get_vnode(&chain, 0); x != NULL; x = get_vnode_next(&chain, x)) {
            q_info[i] = x->node->ds_queue.q_info[x->vid.ds_id];
            io_cnt[i] = x->node->ds_queue.io_cnt[x->vid.ds_id];
            i++;
        }
        struct kv_ds_q_info *y = kv_ds_queue_find(q_info, io_cnt, i, kv_ds_op_cost(KV_DS_GET));
        if (y == NULL) return false;
        struct vid_entry *dst = get_vnode(&chain, y - q_info);
        dst->node->ds_queue.io_cnt[dst->vid.ds_id]++;
        dst->node->ds_queue.q_info[dst->vid.ds_id] = *y;
        ctx->entry = dst;
        dst->node->req_cnt++;
        kv_rdma_send_req(dst->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->resp, ctx->resp_addr, dispatch_send_cb, ctx);
        return true;
    } else {
        struct vid_entry *head = get_vnode(&chain, 0);
        uint32_t cost = kv_ds_op_cost(msg->type == KV_MSG_SET ? KV_DS_SET : KV_DS_DEL);
        struct kv_ds_q_info q_info = head->node->ds_queue.q_info[head->vid.ds_id];
        uint32_t io_cnt = head->node->ds_queue.io_cnt[head->vid.ds_id];
        if (kv_ds_queue_find(&q_info, &io_cnt, 1, cost)) {
            head->node->ds_queue.io_cnt[head->vid.ds_id]++;
            head->node->ds_queue.q_info[head->vid.ds_id] = q_info;
            ctx->entry = head;
            head->node->req_cnt++;
            kv_rdma_send_req(head->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->resp, ctx->resp_addr, dispatch_send_cb, ctx);
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
        entry = get_vnode(key, entry->node->info->rpl_num);  // forward to tail
#else
        entry = get_vnode(key, (random() % entry->node->info->rpl_num) + 1);  // random
#endif
    }
    ctx->entry = entry;
    entry->node->ds_queue.io_cnt[entry->vid->ds_id]++;
    msg->ds_id = entry->vid->ds_id;
    kv_rdma_send_req(entry->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->resp, ctx->resp_addr, dispatch_send_cb, ctx);
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
        if (--x->next_retry) continue;
        if (try_send_req(x)) {
            TAILQ_REMOVE(dp, x, next);
        } else {
            if (x->retry_num < 10) x->retry_num++;
            x->next_retry = 1 << x->retry_num;
        }
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
void kv_ring_dispatch(kv_rdma_mr req, kv_rdma_mr resp, void *resp_addr, kv_ring_cb cb, void *cb_arg) {
    struct kv_ring *self = &g_ring;
    struct dispatch_ctx *ctx = kv_malloc(sizeof(*ctx));
    *ctx = (struct dispatch_ctx){req, resp, resp_addr, cb, cb_arg, kv_app_get_thread_index(), 0, 1};
    if (ctx->thread_id >= self->thread_id && ctx->thread_id < self->thread_id + self->thread_num) {
        dispatch(ctx);
    } else {
        kv_app_send(self->thread_id + random() % self->thread_num, dispatch, ctx);
    }
};

struct forward_ctx {
    struct kv_node *node;
    kv_rdma_mr req;
    kv_rdma_req_cb cb;
    void *cb_arg;
};

static void forward(void *arg) {
    struct forward_ctx *ctx = arg;
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(ctx->req);
    kv_rdma_send_req(ctx->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->req, msg, ctx->cb, ctx->cb_arg);
    free(ctx);
}

void kv_ring_forward(void *node, kv_rdma_mr req, kv_rdma_req_cb cb, void *cb_arg) {
    struct kv_ring *self = &g_ring;
    struct forward_ctx *ctx = kv_malloc(sizeof(*ctx));
    uint32_t thread_id = kv_app_get_thread_index();
    *ctx = (struct forward_ctx){node, req, cb, cb_arg};
    if (thread_id >= self->thread_id && thread_id < self->thread_id + self->thread_num) {
        forward(ctx);
    } else {
        kv_app_send(self->thread_id + random() % self->thread_num, forward, ctx);
    }
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

// --- hash ring: virtual nodes management ---
static inline struct vid_entry *vnode_create(struct ring_change_ctx *ctx, struct vid_ring *ring, struct vid_entry *entry) {
    struct vid_entry *x = kv_malloc(sizeof(*x));
    *x = (struct vid_entry){.vid = ctx->vid, .node = ctx->node, .cp_cnt = 0, .rm_cnt = 0};
    if (entry) {
        CIRCLEQ_INSERT_BEFORE(ring + ctx->ring_id, entry, x, entry);
    } else {
        CIRCLEQ_INSERT_HEAD(ring + ctx->ring_id, x, entry);
    }
    return x;
}

static inline void vnode_delete(struct ring_change_ctx *ctx, struct vid_ring *ring, struct vid_entry *entry) {
    CIRCLEQ_REMOVE(ring + ctx->ring_id, entry, entry);
    kv_free(entry);
}

static void update_ring(void *arg) {
    struct kv_ring *self = &g_ring;
    struct ring_change_ctx *ctx = arg;
    bool master_thread = kv_app_get_thread_index() == self->thread_id;
    if (master_thread && --ctx->cnt) return;
    struct vid_ring *ring = self->rings[kv_app_get_thread_index() - self->thread_id];
    struct vid_entry *vnode = find_vid_by_node(ring + ctx->ring_id, ctx->node);
    switch ((ctx->state << 1) | ctx->msg_type) {
        case (VID_COPYING << 1) | KV_ETCD_MSG_PUT:
            if (vnode == NULL) vnode = vnode_create(ctx, ring, vnode);
            vnode->state = VID_COPYING;
            vnode->cp_cnt++;
            if (master_thread && vnode->node->is_local) {
                static char key[128];
                sprintf(key, "/rings/%u/1/%s/", ctx->ring_id, self->local_id);  // running
                kvEtcdPut(key, &vnode->vid, sizeof(vnode->vid), &self->vid_lease);
            }
            break;
        case (VID_COPYING << 1) | KV_ETCD_MSG_DEL:
            assert(vnode != NULL);
            assert(vnode->state == VID_COPYING);
            if (--vnode->cp_cnt == 0) vnode->state = VID_RUNNING;
            break;
        case (VID_RUNNING << 1) | KV_ETCD_MSG_PUT:
            if (vnode == NULL) vnode = vnode_create(ctx, ring, vnode);
            if (vnode->cp_cnt == 0) vnode->state = VID_RUNNING;
            break;
        case (VID_RUNNING << 1) | KV_ETCD_MSG_DEL:
            assert(vnode != NULL);
            if (vnode->rm_cnt == 0)
                vnode_delete(ctx, ring, vnode);
            else
                assert(vnode->state == VID_LEAVING);
            break;
        case (VID_LEAVING << 1) | KV_ETCD_MSG_PUT:
            if (vnode == NULL) vnode = vnode_create(ctx, ring, vnode);
            vnode->state = VID_LEAVING;
            break;
        case (VID_LEAVING << 1) | KV_ETCD_MSG_DEL:
            assert(vnode != NULL);
            assert(vnode->state == VID_LEAVING);
            if (--vnode->rm_cnt == 0) vnode_delete(ctx, ring, vnode);
            break;
        default:
            assert(false);
    }

    if (!master_thread) {
        kv_app_send(self->thread_id, update_ring, ctx);  // send ack to master thread
    } else {
        kv_free(ctx);  // all the hash rings are updated
    }
};

static void on_ring_change(void *arg) {
    struct kv_ring *self = &g_ring;
    struct ring_change_ctx *ctx = arg;
    ctx->cnt = self->thread_num;
    HASH_FIND_STR(self->nodes, ctx->node_id, ctx->node);
    if (ctx->node == NULL) {
        ctx->node = kv_malloc(sizeof(struct kv_node));
        strcpy(ctx->node->node_id, ctx->node_id);
        ctx->node->has_info = false;
        HASH_ADD_STR(self->nodes, node_id, ctx->node);
        STAILQ_INIT(&ctx->node->ring_updates);
        STAILQ_INSERT_HEAD(&ctx->node->ring_updates, ctx, next);
    } else if (!ctx->node->has_info || !ctx->node->is_connected) {  // node not ready
        STAILQ_INSERT_HEAD(&ctx->node->ring_updates, ctx, next);
    } else {
        for (size_t i = 1; i < self->thread_num; i++) kv_app_send(self->thread_id + i, update_ring, ctx);
        update_ring(ctx);
    }
}

// --- hash ring: nodes management ---
static inline bool are_vnodes_leaving(struct kv_node *node) {
    // ring 0 is the last updated ring.
    struct vid_ring *ring = g_ring.rings[0];
    if (CIRCLEQ_EMPTY(ring)) return NULL;
    struct vid_entry *x;
    CIRCLEQ_FOREACH(x, ring, entry) {
        if (x->node == node && x->state != VID_LEAVING) return false;
    }
    return true;
}
static void rdma_disconnect_cb(void *arg) {
    struct kv_ring *self = &g_ring;
    struct kv_node *node = arg;
    if (node->is_disconnecting) {
        assert(are_vnodes_leaving(node) && node->req_cnt == 0);
        printf("client: disconnected to node %s.\n", node->node_id);
        kv_ds_queue_fini(&node->ds_queue);
        kv_free(node);
    } else {
        // TODO: never retry.
        node->is_connected = false;
        STAILQ_INSERT_TAIL(&self->conn_q, node, next);
    }
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
    struct ring_change_ctx *ctx;
    while ((ctx = STAILQ_FIRST(&node->ring_updates)) != NULL) {
        STAILQ_REMOVE_HEAD(&node->ring_updates, next);
        for (size_t i = 1; i < self->thread_num; i++) kv_app_send(self->thread_id + i, update_ring, ctx);
        update_ring(ctx);
    }
}

static int kv_conn_q_poller(void *arg) {
    struct kv_ring *self = arg;
    struct kv_nodes_head conn_q = STAILQ_HEAD_INITIALIZER(conn_q);
    while (!STAILQ_EMPTY(&self->conn_q)) {
        struct kv_node *node = STAILQ_FIRST(&self->conn_q);
        STAILQ_REMOVE_HEAD(&self->conn_q, next);
        assert(!node->is_local);
        if (node->is_disconnecting) {
            if (node->is_connected) {
                if (are_vnodes_leaving(node) && node->req_cnt == 0) {
                    kv_rdma_disconnect(node->conn);
                } else {
                    STAILQ_INSERT_HEAD(&conn_q, node, next);
                }
            } else {
                rdma_disconnect_cb(node);
            }
        } else {
            assert(!node->is_connected);
            char ip_port[KV_MAX_NODEID_LEN], *p = ip_port;
            strcpy(ip_port, node->node_id);
            while (*(p++) != ':') assert(p - ip_port < KV_MAX_NODEID_LEN);
            *(p - 1) = '\0';
            kv_rdma_connect(self->h, ip_port, p, rdma_connect_cb, node, rdma_disconnect_cb, node);
        }
    }
    self->conn_q = conn_q;
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
        // TODO: wait 10 seconds for the ongoing requests
        exit(-1);
    }
    node->is_disconnecting = true;
    if (node->is_connected) STAILQ_INSERT_TAIL(&self->conn_q, node, next);
    // TODO: tail-> write leaving ring
finish:
    kv_free(node_id);
}

static void on_node_put(void *arg) {
    struct kv_node *node = arg, *tmp = NULL;
    struct kv_ring *self = &g_ring;
    ring_init(node->info.ring_num);
    HASH_FIND_STR(self->nodes, node->node_id, tmp);
    if (tmp != NULL) {  // this node already exist
        if (tmp->has_info == true) {
            kv_free(node);
            return;
        }
        tmp->info = node->info;
        kv_free(node);
        node = tmp;
    }
    node->has_info = true;
    node->is_local = !strcmp(self->local_id, node->node_id);
    node->is_disconnecting = false;
    node->is_connected = false;
    node->req_cnt = 0;
    kv_ds_queue_init(&node->ds_queue, node->info.ds_num);
    for (uint32_t i = 0; i < node->info.ds_num; ++i) {
        node->ds_queue.q_info[i] = (struct kv_ds_q_info){0, 0};
        node->ds_queue.io_cnt[i] = 0;
    }
    HASH_ADD_STR(self->nodes, node_id, node);
    if (!node->is_local) STAILQ_INSERT_TAIL(&self->conn_q, node, next);
}

static inline const char *key_copy(char *dst, const char *src) {
    while (*src != '/') *(dst++) = *(src++);
    *dst = '\0';
    return src;
}
static inline bool key_cmp(const char *s1, const char *s2) {
    while (*s1 != '\0' && *s2 != '/')
        if (*(s1++) != *(s2++)) return false;
    return true;
}
static inline const char *key_next(const char *s1) {
    while (*(s1++) != '/')
        ;
    return s1;
}

static void msg_handler(enum kv_etcd_msg_type msg, const char *key, uint32_t key_len, const void *val, uint32_t val_len) {
    // if msg == KV_ETCD_MSG_DEL, val_len == 0
    struct kv_ring *self = &g_ring;
    if (*(key++) != '/') return;
    if (key_cmp("nodes", key)) {
        key = key_next(key);
        if (msg == KV_ETCD_MSG_PUT) {
            struct kv_node *node = kv_malloc(sizeof(struct kv_node));
            key_copy(node->node_id, key);
            assert(val_len == sizeof(struct kv_etcd_node));
            kv_memcpy(&node->info, val, val_len);
            printf("PUT NODE %s\n", node->node_id);
            kv_app_send_without_token(self->thread_id, on_node_put, node);
        } else if (msg == KV_ETCD_MSG_DEL) {
            char *nodeID = kv_malloc(KV_MAX_NODEID_LEN);
            key_copy(nodeID, key);
            printf("DEL NODE %s\n", nodeID);
            kv_app_send_without_token(self->thread_id, on_node_del, nodeID);
        }
    } else if (key_cmp("rings", key)) {
        struct ring_change_ctx *ctx = kv_malloc(sizeof(*ctx));
        key = key_next(key);
        sscanf(key, "%u", &ctx->ring_id);
        key = key_next(key);
        sscanf(key, "%u", &ctx->state);
        key = key_next(key);
        key_copy(ctx->node_id, key);
        printf("%s %uth RING: %u %s\n", msg == KV_ETCD_MSG_PUT ? "PUT" : "DEL", ctx->ring_id, ctx->state, ctx->node_id);
        if (msg == KV_ETCD_MSG_PUT) {
            assert(val_len == sizeof(ctx->vid));
            kv_memcpy(&ctx->vid, val, val_len);
        }
        ctx->msg_type = msg;
        kv_app_send_without_token(self->thread_id, on_ring_change, ctx);
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
    kvEtcdInit(etcd_ip, etcd_port, msg_handler);
    self->conn_q_poller = kv_app_poller_register(kv_conn_q_poller, self, 1000000);
    return self->h;
}

void kv_ring_req_fini(void *_node) {
    struct kv_node *node = _node;
    if (node) node->req_cnt--;
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
static void rdma_req_handler_wrapper(void *req_h, kv_rdma_mr req, uint32_t req_sz, void *arg) {
    struct kv_ring *self = &g_ring;
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(req);
    struct vnode_chain chain;
    if (get_chain(&chain, KV_MSG_KEY(msg)) == false) {
        fprintf(stderr, "can not get the hash chain!\n");
        exit(-1);
    }
    if (msg->type == KV_MSG_SET || msg->type == KV_MSG_DEL) {
        struct vid_entry *local = get_vnode(&chain, msg->hop - 1);
        if (!local->node->is_local) goto send_nak;
        struct vid_entry *next = get_vnode_next(&chain, local);
        if (next) {
            msg->hop++;
            next->node->req_cnt++;
        }
        self->req_handler(req_h, req, req_sz, local->vid.ds_id, next ? next->node : NULL, arg);
        return;
    } else if (msg->type == KV_MSG_GET) {
        if (msg->hop == 1) {
            struct vid_entry *x;
            for (x = get_vnode(&chain, 0); x != NULL; x = get_vnode_next(&chain, x))
                if (x->node->is_local) break;
            if (x == NULL) goto send_nak;
            struct vid_entry *tail = get_vnode_from_chain(&chain, -1, x, false);
            msg->hop++;
            tail->node->req_cnt++;
            self->req_handler(req_h, req, req_sz, x->vid.ds_id, tail->node->is_local ? NULL : tail->node, arg);
            return;
        } else if (msg->hop == 2) {
            struct vid_entry *tail = get_vnode(&chain, -1);
            if (!tail->node->is_local) goto send_nak;
            self->req_handler(req_h, req, req_sz, tail->vid.ds_id, NULL, arg);
            return;
        }
    }
    assert(false);
send_nak:
    msg->type = KV_MSG_OUTDATED;
    msg->value_len = 0;
    kv_rdma_make_resp(req_h, (uint8_t *)msg, KV_MSG_SIZE(msg));
}

void kv_ring_server_init(char *local_ip, char *local_port, uint32_t ring_num, uint32_t vid_per_ssd, uint32_t ds_num,
                         uint32_t rpl_num, uint32_t con_req_num, uint32_t max_msg_sz, kv_ring_req_handler handler, void *arg,
                         kv_rdma_server_init_cb cb, void *cb_arg) {
    assert(ds_num != 0);
    assert(vid_per_ssd * ds_num <= ring_num);
    assert(local_ip && local_port);
    struct kv_ring *self = &g_ring;
    ring_init(ring_num);
    // all vnodes in the hash ring must be valid

    self->req_handler = handler;
    kv_rdma_listen(self->h, local_ip, local_port, con_req_num, max_msg_sz, rdma_req_handler_wrapper, arg, cb, cb_arg);  // TODO: change this
    sprintf(self->local_id, "%s:%s", local_ip, local_port);
    static char key[128];
    sprintf(key, "/nodes/%s/", self->local_id);
    self->node_lease = kvEtcdLeaseCreate(1, true);
    struct kv_etcd_node etcd_node = {.ds_num = ds_num, .ring_num = ring_num, .rpl_num = rpl_num};
    kvEtcdPut(key, &etcd_node, sizeof(etcd_node), &self->node_lease);

    struct vid_ring_stat stats[ring_num];
    for (size_t i = 0; i < ring_num; i++) stats[i] = (struct vid_ring_stat){i, ring_size(self->rings[0] + i)};
    qsort(stats, ring_num, sizeof(struct vid_ring_stat), vid_ring_stat_cmp);
    self->vid_lease = kvEtcdLeaseCreate(5, true);  // vid lease ttl must larger than node lease ttl
    uint64_t init_lease = kvEtcdLeaseCreate(8, false);

    uint32_t ds_id = 0;
    for (size_t i = 0; i < vid_per_ssd * ds_num; i++) {
        struct kv_etcd_vid vid = {.ds_id = ds_id};
        // random_vid(vid.vid);
        hash_vid(vid.vid, local_ip, local_port, i);
        sprintf(key, "/rings/%u/0/%s/", stats[i].index, self->local_id);  // copying
        kvEtcdPut(key, &vid, sizeof(vid), &init_lease);

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