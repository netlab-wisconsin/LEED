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

#define STAILQ_FOREACH_SAFE(var, head, field, tvar) \
    for ((var) = STAILQ_FIRST((head)); (var) && ((tvar) = STAILQ_NEXT((var), field), 1); (var) = (tvar))

// etcd values
#define MAX_ETCD_KEY_LEN (128U)
struct kv_etcd_node {
    uint32_t log_ring_num;
    uint32_t log_bkt_num;
    uint16_t rpl_num;
    uint16_t ds_num;
} __attribute__((packed));

enum { VID_JOINING,
       VID_RUNNING,
       VID_LEAVING };
struct kv_etcd_vid {
#define KV_VID_LEN (20U)
    uint16_t ds_id;
    uint8_t vid[KV_VID_LEN];
} __attribute__((packed));

//---- rings ----
#define KV_MAX_NODEID_LEN (24U)
struct ring_change_ctx {
    struct kv_etcd_vid vid;
    char node_id[KV_MAX_NODEID_LEN];
    char src_id[KV_MAX_NODEID_LEN];
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
    uint32_t cp_cnt, rm_cnt;
    CIRCLEQ_ENTRY(vid_entry)
    entry;
};

struct vnode_ring {
    CIRCLEQ_HEAD(, vid_entry)
    head;
    uint64_t version;
    TAILQ_HEAD(, copy_range_ctx)
    copy_ranges;
    uint32_t range_size;
};

struct copy_range_ctx {
    struct kv_ring_copy_info info;
    struct vnode_ring *ring;
    struct vid_entry *invalid;
    uint64_t version;
    TAILQ_ENTRY(copy_range_ctx)
    entry;
};

struct dispatch_ctx {
    kv_rdma_mr req;
    kv_rdma_mr resp;
    void *resp_addr;
    kv_ring_cb cb;
    void *cb_arg;
    uint32_t thread_id;
    uint32_t retry_num, next_retry;
    struct kv_node *node;
    uint32_t ds_id;
    TAILQ_ENTRY(dispatch_ctx)
    next;
};
TAILQ_HEAD(dispatch_queue, dispatch_ctx);

#define RING_VERSION_MAX 64
struct ring_version_t {
    _Atomic uint64_t counter;
};

struct kv_ring {
    kv_rdma_handle h;
    uint32_t thread_id, thread_num;
    char local_id[KV_MAX_NODEID_LEN];
    struct kv_node *nodes;
    struct vnode_ring **rings;
    struct dispatch_queue *dqs;
    void **dq_pollers;
    uint32_t log_ring_num;
    struct kv_nodes_head conn_q;
    void *conn_q_poller;
    kv_ring_cb server_online_cb;
    void *arg;
    kv_ring_req_handler req_handler;
    uint64_t node_lease, vid_lease;
    _Atomic bool is_server_exiting;
    kv_ring_copy_cb copy_cb;
    void *copy_cb_arg;
    struct ring_version_t (*rings_version)[RING_VERSION_MAX];
} g_ring;

static void random_vid(char *vid) __attribute__((unused));
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
static inline size_t ring_size(struct vnode_ring *ring) {
    if (CIRCLEQ_EMPTY(&ring->head)) return 0;
    size_t size = 0;
    struct vid_entry *x;
    CIRCLEQ_FOREACH(x, &ring->head, entry) {
        size++;
    }
    return size;
}

static inline uint64_t get_vid_64(uint8_t *vid) { return *(uint64_t *)vid; }
static inline uint32_t get_ring_id(uint8_t *vid, uint32_t log_ring_num) {
    if (log_ring_num == 0) return 0;
    return get_vid_64(vid) >> (64 - log_ring_num);
}
static inline void set_ring_id(uint8_t *vid, uint32_t log_ring_num, uint32_t ring_id) {
    if (log_ring_num == 0) return;
    *(uint64_t *)vid &= ((1ULL << (64 - log_ring_num)) - 1);
    *(uint64_t *)vid |= (uint64_t)ring_id << (64 - log_ring_num);
}
static inline void mask_vnode_id(uint8_t *vid, uint32_t log_bkt_num) {
    uint64_t mask = (1ULL << (64 - log_bkt_num)) - 1;
    *(uint64_t *)vid |= mask;
}
// keys from (vnode_a, vnode_b] are belong to b.
static struct vid_entry *find_vid_entry(struct vnode_ring *ring, char *vid) {
    if (CIRCLEQ_EMPTY(&ring->head)) return NULL;
    uint64_t base_vid = get_vid_64(CIRCLEQ_LAST(&ring->head)->vid.vid) + 1;
    uint64_t d = get_vid_64(vid) - base_vid;
    struct vid_entry *x;
    CIRCLEQ_FOREACH(x, &ring->head, entry) {
        if (get_vid_64(x->vid.vid) - base_vid >= d) break;
    }
    assert(x != (const void *)(&ring->head));
    return x;
}

static struct vid_entry *find_vid_by_node(struct vnode_ring *ring, struct kv_node *node) {
    if (CIRCLEQ_EMPTY(&ring->head)) return NULL;
    struct vid_entry *x;
    CIRCLEQ_FOREACH(x, &ring->head, entry) {
        assert(x->node);
        if (x->node == node)
            return x;
    }
    return NULL;
}

struct vnode_chain {
    struct vnode_ring *ring;
    struct vid_entry *base, *copy, *invalid;  // copy may point to a invalid vnode
    uint32_t rpl_num;
    struct vid_entry *vids[0];
};

static struct vnode_chain *get_chain(char *key) {
    struct kv_ring *self = &g_ring;
    if (self->rings == NULL) return NULL;
    struct vnode_ring *ring = self->rings[kv_app_get_thread_index() - self->thread_id];
    ring = ring + get_ring_id(key, self->log_ring_num);
    struct vid_entry *base = find_vid_entry(ring, key);
    if (base == NULL) return NULL;
    uint32_t rpl_num = base->node->info.rpl_num;
    struct vnode_chain *chain = kv_malloc(sizeof(struct vnode_chain) + sizeof(struct vid_entry *) * rpl_num);
    *chain = (struct vnode_chain){ring, base, NULL, NULL, 0};
    struct vid_entry *x = chain->base;
    while (chain->rpl_num < rpl_num) {
        if (x->state == VID_RUNNING) {
            chain->vids[chain->rpl_num] = x;
            chain->rpl_num++;
        } else {
            if (chain->invalid != NULL) {
                fprintf(stderr, "two failed vnodes in a hash ring!\n");
                exit(-1);
            }
            chain->invalid = x;
            if (chain->invalid->state == VID_LEAVING) rpl_num--;
        }
        if ((x = CIRCLEQ_LOOP_NEXT(&chain->ring->head, x, entry)) == chain->base) break;
    }
    if (chain->invalid != NULL) {
        if (chain->invalid->state == VID_LEAVING && x != chain->base)
            chain->copy = x;  // pre_copy to the next vnode
        if (chain->invalid->state == VID_JOINING)
            chain->copy = chain->invalid;  // pre_copy to new vnode.
    }
    return chain;
}

// --- dispatch ---
#define MAX_RETRY_NUM 10

static void dispatch_send_cb(connection_handle h, bool success, kv_rdma_mr req, kv_rdma_mr resp, void *cb_arg) {
    struct kv_ring *self = &g_ring;
    struct dispatch_ctx *ctx = cb_arg;
    struct kv_msg *msg = ctx->resp_addr;
    ctx->node->ds_queue.io_cnt[ctx->ds_id]--;
    ctx->node->ds_queue.q_info[ctx->ds_id] = msg->q_info;
    ctx->node->req_cnt--;
    if (success && msg->type == KV_MSG_OUTDATED) {
        struct dispatch_queue *dp = &self->dqs[kv_app_get_thread_index() - self->thread_id];
        if (ctx->retry_num < MAX_RETRY_NUM) ctx->retry_num++;
        ctx->next_retry = 1 << ctx->retry_num;
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
    struct vnode_chain *chain = get_chain(KV_MSG_KEY(msg));
    if (chain == NULL) return false;
    msg->hop = 1;
    if (msg->type == KV_MSG_GET) {
        struct kv_ds_q_info q_info[chain->rpl_num];
        uint32_t io_cnt[chain->rpl_num];
        uint32_t i = 0;
        for (; i < chain->rpl_num; i++) {
            struct vid_entry *x = chain->vids[i];
            q_info[i] = x->node->ds_queue.q_info[x->vid.ds_id];
            io_cnt[i] = x->node->ds_queue.io_cnt[x->vid.ds_id];
        }
        struct kv_ds_q_info *y = kv_ds_queue_find(q_info, io_cnt, i, kv_ds_op_cost(KV_DS_GET));
        if (y == NULL) {
            kv_free(chain);
            return false;
        }
        struct vid_entry *dst = chain->vids[y - q_info];
        ctx->ds_id = dst->vid.ds_id;
        ctx->node = dst->node;
        ctx->node->ds_queue.io_cnt[ctx->ds_id]++;
        ctx->node->ds_queue.q_info[ctx->ds_id] = *y;
        ctx->node->req_cnt++;
        kv_rdma_send_req(dst->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->resp, ctx->resp_addr, dispatch_send_cb, ctx);
        kv_free(chain);
        return true;
    } else {
        if (chain->rpl_num == 0) {
            kv_free(chain);
            return false;
        }
        struct vid_entry *head = chain->vids[0];
        uint32_t cost = kv_ds_op_cost(msg->type == KV_MSG_SET ? KV_DS_SET : KV_DS_DEL);
        struct kv_ds_q_info q_info = head->node->ds_queue.q_info[head->vid.ds_id];
        uint32_t io_cnt = head->node->ds_queue.io_cnt[head->vid.ds_id];
        if (kv_ds_queue_find(&q_info, &io_cnt, 1, cost)) {
            ctx->ds_id = head->vid.ds_id;
            ctx->node = head->node;
            ctx->node->ds_queue.io_cnt[ctx->ds_id]++;
            ctx->node->ds_queue.q_info[ctx->ds_id] = q_info;
            ctx->node->req_cnt++;
            kv_rdma_send_req(head->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->resp, ctx->resp_addr, dispatch_send_cb, ctx);
            kv_free(chain);
            return true;
        }
        kv_free(chain);
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
        assert(x->next_retry);
        if (--x->next_retry) continue;
        if (try_send_req(x)) {
            TAILQ_REMOVE(dp, x, next);
        } else {
            if (x->retry_num < MAX_RETRY_NUM) x->retry_num++;
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
    bool is_copy_req;
    kv_ring_cb cb;
    void *cb_arg;
    uint32_t thread_id;
    struct ring_version_t *ring_version;
};

static void forward_cb(connection_handle h, bool success, kv_rdma_mr req, kv_rdma_mr resp, void *cb_arg) {
    struct forward_ctx *ctx = cb_arg;
    ctx->node->req_cnt--;
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_resp_buf(resp);
    if (!success) msg->type = KV_MSG_ERR;
    if (ctx->cb) kv_app_send(ctx->thread_id, ctx->cb, ctx->cb_arg);
    if (!ctx->is_copy_req) ctx->ring_version->counter--;
    kv_free(ctx);
}

static void forward(void *arg) {
    struct forward_ctx *ctx = arg;
    struct kv_msg *msg = (struct kv_msg *)kv_rdma_get_req_buf(ctx->req);
    if (ctx->is_copy_req) {
        assert(ctx->node == NULL);
        struct vnode_chain *chain = get_chain(KV_MSG_KEY(msg));
        assert(chain->copy);
        ctx->node = chain->copy->node;
        msg->hop = chain->rpl_num + 1;
        ctx->node->req_cnt++;
        kv_free(chain);
    }
    kv_rdma_send_req(ctx->node->conn, ctx->req, KV_MSG_SIZE(msg), ctx->req, msg, forward_cb, ctx);
}

void kv_ring_forward(void *_ctx, kv_rdma_mr req, bool is_copy_req, kv_ring_cb cb, void *cb_arg) {
    struct kv_ring *self = &g_ring;
    struct forward_ctx *ctx = _ctx;
    if (is_copy_req) {
        assert(ctx == NULL);
        ctx = kv_malloc(sizeof(*ctx));
        ctx->node = NULL;
    } else if (req == NULL || ctx->node == NULL) {
        if (ctx->node) ctx->node->req_cnt--;
        ctx->ring_version->counter--;
        kv_free(ctx);
        if (cb) cb(cb_arg);
        return;
    }

    ctx->req = req;
    ctx->is_copy_req = is_copy_req;
    ctx->cb = cb;
    ctx->cb_arg = cb_arg;
    ctx->thread_id = kv_app_get_thread_index();
    if (ctx->thread_id >= self->thread_id && ctx->thread_id < self->thread_id + self->thread_num) {
        forward(ctx);
    } else {
        kv_app_send(self->thread_id + random() % self->thread_num, forward, ctx);
    }
}

static void ring_init(uint32_t log_ring_num) {
    struct kv_ring *self = &g_ring;
    if (self->rings == NULL) {
        self->log_ring_num = log_ring_num;
        self->rings = kv_calloc(self->thread_num, sizeof(struct vnode_ring *));
        self->rings_version = kv_calloc(1u << self->log_ring_num, sizeof(*self->rings_version));
        for (size_t i = 0; i < 1u << self->log_ring_num; i++) {
            for (size_t j = 0; j < RING_VERSION_MAX; j++) self->rings_version[i][j].counter = 0;
        }
        for (size_t i = 0; i < self->thread_num; i++) {
            self->rings[i] = kv_calloc(1u << self->log_ring_num, sizeof(struct vnode_ring));
            for (size_t j = 0; j < 1u << self->log_ring_num; j++) {
                CIRCLEQ_INIT(&self->rings[i][j].head);
                self->rings[i][j].version = 0;
                TAILQ_INIT(&self->rings[i][j].copy_ranges);
                self->rings[i][j].range_size = 0;
            }
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
static inline struct vid_entry *vnode_create(struct ring_change_ctx *ctx, struct vnode_ring *ring) {
    struct vid_entry *x = kv_malloc(sizeof(*x));
    *x = (struct vid_entry){.vid = ctx->vid, .node = ctx->node, .cp_cnt = 0, .rm_cnt = 0};
    struct vid_entry *entry = find_vid_entry(ring + ctx->ring_id, ctx->vid.vid);
    if (entry) {
        CIRCLEQ_INSERT_BEFORE(&ring[ctx->ring_id].head, entry, x, entry);
    } else {
        CIRCLEQ_INSERT_HEAD(&ring[ctx->ring_id].head, x, entry);
    }
    return x;
}

static inline void vnode_delete(struct ring_change_ctx *ctx, struct vnode_ring *ring, struct vid_entry *entry) {
    CIRCLEQ_REMOVE(&ring[ctx->ring_id].head, entry, entry);
    kv_free(entry);
}

static bool vnode_join_copyable(struct vnode_ring *ring, struct vid_entry *vid) {
    struct vid_entry *x = vid;
    for (size_t i = 0; i < vid->node->info.rpl_num; i++) {
        x = CIRCLEQ_LOOP_NEXT(&ring->head, x, entry);
        if (x == vid) break;
        if (x->node->is_local) return true;
    }
    return false;
}
static bool vnode_leave_copyable(struct vnode_ring *ring, struct vid_entry *vid) {
    if (CIRCLEQ_LOOP_PREV(&ring->head, vid, entry)->node->is_local) return true;
    uint32_t len = vid->node->info.rpl_num - 1;
    for (size_t i = 0; i < len; i++) {
        vid = CIRCLEQ_LOOP_NEXT(&ring->head, vid, entry);
        if (vid->node->is_local) return true;
    }
    return false;
}

static void start_copy(struct vnode_ring *ring, struct vid_entry *vnode) {
    struct kv_ring *self = &g_ring;
    struct vid_entry *x = vnode;
    for (size_t i = 0; i < vnode->node->info.rpl_num; i++) {
        struct vnode_chain *chain = get_chain(x->vid.vid);
        struct vid_entry *tail = chain->vids[chain->rpl_num - 1];
        if (chain->copy && tail->node->is_local && self->copy_cb) {
            // the local node must be the tail of the hash chain
            bool del = chain->rpl_num == vnode->node->info.rpl_num;
            // since the datastore is unaware of the multiple rings, we may need to split the key range.
            uint64_t start = get_vid_64(CIRCLEQ_LOOP_PREV(&ring->head, chain->base, entry)->vid.vid) + 1;
            uint64_t end = get_vid_64(chain->base->vid.vid) + 1;
            if (start > end && end != 0) {
                uint64_t ring_start = 0, ring_end = 0, ring_id = get_ring_id(chain->base->vid.vid, self->log_ring_num);
                set_ring_id((uint8_t *)&ring_start, self->log_ring_num, ring_id);
                set_ring_id((uint8_t *)&ring_end, self->log_ring_num, ring_id + 1);
                ring->range_size += 2;
                struct copy_range_ctx *ctx[2] = {kv_malloc(sizeof(struct copy_range_ctx)), kv_malloc(sizeof(struct copy_range_ctx))};
                *ctx[0] = (struct copy_range_ctx){{ring_start, end, tail->vid.ds_id, del}, ring, chain->invalid, ring->version};
                *ctx[1] = (struct copy_range_ctx){{start, ring_end, tail->vid.ds_id, del}, ring, chain->invalid, ring->version};
                self->copy_cb(true, &ctx[0]->info, self->copy_cb_arg);
                self->copy_cb(true, &ctx[1]->info, self->copy_cb_arg);
            } else {
                ring->range_size++;
                struct copy_range_ctx *ctx = kv_malloc(sizeof(struct copy_range_ctx));
                *ctx = (struct copy_range_ctx){{start, end, tail->vid.ds_id, del}, ring, chain->invalid, ring->version};
                self->copy_cb(true, &ctx->info, self->copy_cb_arg);
            }
        }
        kv_free(chain);
        if ((x = CIRCLEQ_LOOP_PREV(&ring->head, x, entry)) == vnode) break;
    }
}

static void update_ring(void *arg) {
    struct kv_ring *self = &g_ring;
    struct ring_change_ctx *ctx = arg;
    bool master_thread = kv_app_get_thread_index() == self->thread_id;
    static char key[MAX_ETCD_KEY_LEN];
    if (master_thread)
        if (--ctx->cnt) return;
    struct vnode_ring *ring = self->rings[kv_app_get_thread_index() - self->thread_id];
    struct vid_entry *vnode = find_vid_by_node(ring + ctx->ring_id, ctx->node);
    switch ((ctx->state << 1) | ctx->msg_type) {
        case (VID_JOINING << 1) | KV_ETCD_MSG_PUT:
            if (vnode == NULL) vnode = vnode_create(ctx, ring);
            vnode->state = VID_JOINING;
            vnode->cp_cnt++;
            if (master_thread) {
                if (vnode->node->is_local && strcmp(ctx->src_id, self->local_id) == 0) {
                    sprintf(key, "/rings/%u/1/%s/", ctx->ring_id, self->local_id);  // running
                    kvEtcdPut(key, &vnode->vid, sizeof(vnode->vid), &self->vid_lease);
                } else if (!vnode->node->is_local && strcmp(ctx->src_id, ctx->node_id) == 0) {
                    if (vnode_join_copyable(ring + ctx->ring_id, vnode)) {
                        sprintf(key, "/rings/%u/0/%s/%s/", ctx->ring_id, vnode->node->node_id, self->local_id);  // joining
                        kvEtcdPut(key, &vnode->vid, sizeof(vnode->vid), &self->vid_lease);
                    }
                } else if (!vnode->node->is_local && strcmp(ctx->src_id, self->local_id) == 0) {
                    start_copy(ring + ctx->ring_id, vnode);
                }
            }
            break;
        case (VID_JOINING << 1) | KV_ETCD_MSG_DEL:
            assert(vnode != NULL);
            assert(vnode->state == VID_JOINING);
            if (--vnode->cp_cnt == 0) {
                vnode->state = VID_RUNNING;
                assert(self->rings_version[ctx->ring_id][(ring[ctx->ring_id].version + RING_VERSION_MAX - 1) % RING_VERSION_MAX].counter == 0);
                ring[ctx->ring_id].version = (ring[ctx->ring_id].version + 1) % RING_VERSION_MAX;
            }
            break;
        case (VID_RUNNING << 1) | KV_ETCD_MSG_PUT:
            if (vnode == NULL) vnode = vnode_create(ctx, ring);
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
            if (vnode == NULL) vnode = vnode_create(ctx, ring);
            vnode->state = VID_LEAVING;
            vnode->rm_cnt++;
            if (master_thread) {
                if (!vnode->node->is_local && strcmp(ctx->src_id, self->local_id) == 0) {
                    start_copy(ring + ctx->ring_id, vnode);
                }
            }
            break;
        case (VID_LEAVING << 1) | KV_ETCD_MSG_DEL:
            assert(vnode != NULL);
            assert(vnode->state == VID_LEAVING);
            if (--vnode->rm_cnt == 0) {
                vnode_delete(ctx, ring, vnode);
                assert(self->rings_version[ctx->ring_id][(ring[ctx->ring_id].version + RING_VERSION_MAX - 1) % RING_VERSION_MAX].counter == 0);
                ring[ctx->ring_id].version = (ring[ctx->ring_id].version + 1) % RING_VERSION_MAX;
            }
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
static void update_rings(struct kv_node *node) {
    struct kv_ring *self = &g_ring;
    struct ring_change_ctx *ctx;
    while ((ctx = STAILQ_FIRST(&node->ring_updates)) != NULL) {
        STAILQ_REMOVE_HEAD(&node->ring_updates, next);
        mask_vnode_id(ctx->vid.vid, node->info.log_bkt_num);
        assert(ctx->msg_type == KV_ETCD_MSG_DEL || ctx->ring_id == get_ring_id(ctx->vid.vid, self->log_ring_num));
        for (size_t i = 1; i < self->thread_num; i++) kv_app_send(self->thread_id + i, update_ring, ctx);
        update_ring(ctx);
    }
}

static void on_ring_change(void *arg) {
    struct kv_ring *self = &g_ring;
    struct ring_change_ctx *ctx = arg;
    bool update_now = true;
    ctx->cnt = self->thread_num;
    HASH_FIND_STR(self->nodes, ctx->node_id, ctx->node);
    if (ctx->node == NULL) {
        ctx->node = kv_malloc(sizeof(struct kv_node));
        strcpy(ctx->node->node_id, ctx->node_id);
        ctx->node->has_info = false;
        HASH_ADD_STR(self->nodes, node_id, ctx->node);
        STAILQ_INIT(&ctx->node->ring_updates);
        update_now = false;
    } else if (!ctx->node->has_info || (!ctx->node->is_local && !ctx->node->is_disconnecting && !ctx->node->is_connected)) {  // node not ready
        update_now = false;
    }
    STAILQ_INSERT_TAIL(&ctx->node->ring_updates, ctx, next);
    if (update_now) update_rings(ctx->node);
}

// --- hash ring: nodes management ---
static inline bool are_vnodes_gone(struct kv_node *node, bool is_leaving) {
    struct kv_ring *self = &g_ring;
    for (uint32_t i = 0; i < 1u << self->log_ring_num; i++) {
        // ring 0 is the last updated ring.
        struct vnode_ring *ring = self->rings[0] + i;
        struct vid_entry *vid = find_vid_by_node(ring, node);
        if (vid) {
            if (is_leaving && vid->state == VID_LEAVING) continue;
            return false;
        }
    }
    return true;
}
static void rdma_disconnect_cb(void *arg) {
    struct kv_ring *self = &g_ring;
    struct kv_node *node = arg;
    if (node->is_disconnecting) {
        assert(are_vnodes_gone(node, true) && node->req_cnt == 0);
        printf("client: disconnected to node %s.\n", node->node_id);
        node->is_connected = false;
        STAILQ_INSERT_TAIL(&self->conn_q, node, next);
    } else {
        fprintf(stderr, "server %s disconnected actively, exiting\n", node->node_id);
        exit(-1);
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
    update_rings(node);
    if (self->server_online_cb) {
        self->server_online_cb(self->arg);
        self->server_online_cb = NULL;  // only call server_online_cb once
    }
}

static int kv_conn_q_poller(void *arg) {
    struct kv_ring *self = arg;
    struct kv_node *node, *tmp;
    STAILQ_FOREACH_SAFE(node, &self->conn_q, next, tmp) {
        if (node->is_disconnecting) {
            if (are_vnodes_gone(node, true) && node->req_cnt == 0) {
                if (node->is_connected) {
                    kv_rdma_disconnect(node->conn);
                    STAILQ_REMOVE(&self->conn_q, node, kv_node, next);
                } else if (are_vnodes_gone(node, false)) {
                    printf("node %s deleted.\n", node->node_id);
                    STAILQ_REMOVE(&self->conn_q, node, kv_node, next);
                    kv_ds_queue_fini(&node->ds_queue);
                    HASH_DEL(self->nodes, node);
                    kv_free(node);
                }
            }
        } else {
            assert(!node->is_connected);
            char ip_port[KV_MAX_NODEID_LEN], *p = ip_port;
            strcpy(ip_port, node->node_id);
            while (*(p++) != ':') assert(p - ip_port < KV_MAX_NODEID_LEN);
            *(p - 1) = '\0';
            kv_rdma_connect(self->h, ip_port, p, rdma_connect_cb, node, rdma_disconnect_cb, node);
            STAILQ_REMOVE(&self->conn_q, node, kv_node, next);
        }
    }
    if (self->is_server_exiting) {
        if (kv_rdma_conn_num(self->h) != 0) return 0;
        for (struct kv_node *x = self->nodes; x != NULL; x = x->hh.next)
            if (x->req_cnt != 0) return 0;
        kv_ring_fini(NULL, NULL);
        exit(0);  // TODO: call finish callback
    }
    for (size_t i = 0; i < 1u << self->log_ring_num; i++) {
        struct vnode_ring *ring = self->rings[0] + i;
        struct copy_range_ctx *ctx, *tmp;
        TAILQ_FOREACH_SAFE(ctx, &ring->copy_ranges, entry, tmp) {
            if (ctx->version == ring->version) continue;
            if (self->rings_version[i][ctx->version].counter == 0) {
                assert(ring->range_size == 0);
                self->copy_cb(false, &ctx->info, self->copy_cb_arg);
                TAILQ_REMOVE(&ring->copy_ranges, ctx, entry);
                kv_free(ctx);
            }
        }
    }
    return 0;
}
static void on_node_del(void *arg) {
    char *node_id = arg;
    struct kv_ring *self = &g_ring;
    struct kv_node *node = NULL;
    HASH_FIND_STR(self->nodes, node_id, node);
    if (node == NULL) goto finish;
    if (node->is_local) {
        fprintf(stderr, "kv_etcd keepalive timeout!\n");
        kvEtcdLeaseRevoke(self->node_lease);
        kvEtcdLeaseRevoke(self->vid_lease);
        self->is_server_exiting = true;
        goto finish;
    }
    node->is_disconnecting = true;
    if (node->is_connected) STAILQ_INSERT_TAIL(&self->conn_q, node, next);

    // tail-> write leaving ring
    for (uint32_t i = 0; i < 1u << self->log_ring_num; i++) {
        struct vnode_ring *ring = self->rings[0] + i;
        if (ring_size(ring) <= node->info.rpl_num) continue;
        struct vid_entry *vid = find_vid_by_node(ring, node);
        if (vid == NULL) continue;
        if (!vnode_leave_copyable(ring, vid)) continue;
        static char key[MAX_ETCD_KEY_LEN];
        sprintf(key, "/rings/%u/2/%s/%s/", i, vid->node->node_id, self->local_id);  // leaving
        kvEtcdPut(key, &vid->vid, sizeof(vid->vid), &self->vid_lease);
    }

finish:
    kv_free(node_id);
}

static void on_node_put(void *arg) {
    struct kv_node *node = arg, *tmp = NULL;
    struct kv_ring *self = &g_ring;
    ring_init(node->info.log_ring_num);
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
    if (node->is_local) {
        update_rings(node);
    } else
        STAILQ_INSERT_TAIL(&self->conn_q, node, next);
}

static inline const char *key_copy(char *dst, const char *src) {
    while (*src != '/') *(dst++) = *(src++);
    *dst = '\0';
    return src + 1;
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
    const char *key_end = key + key_len;
    assert(*(key_end - 1) == '/');
    struct kv_ring *self = &g_ring;
    if (*(key++) != '/') return;
    if (key_cmp("nodes", key)) {
        key = key_next(key);
        if (msg == KV_ETCD_MSG_PUT) {
            struct kv_node *node = kv_malloc(sizeof(struct kv_node));
            key_copy(node->node_id, key);
            assert(val_len == sizeof(struct kv_etcd_node));
            kv_memcpy(&node->info, val, val_len);
            STAILQ_INIT(&node->ring_updates);
            printf("PUT NODE %s\n", node->node_id);
            kv_app_send_without_token(self->thread_id, on_node_put, node);
        } else if (msg == KV_ETCD_MSG_DEL) {
            char *nodeID = kv_malloc(KV_MAX_NODEID_LEN);
            key_copy(nodeID, key);
            printf("DEL NODE %s\n", nodeID);
            kv_app_send_without_token(self->thread_id, on_node_del, nodeID);
        }
    } else if (key_cmp("rings", key)) {
        static const char *state_str[] = {"JOINING", "RUNNING", "LEAVING"};
        struct ring_change_ctx *ctx = kv_malloc(sizeof(*ctx));
        key = key_next(key);
        sscanf(key, "%u", &ctx->ring_id);
        key = key_next(key);
        sscanf(key, "%u", &ctx->state);
        key = key_next(key);
        key = key_copy(ctx->node_id, key);
        if (key == key_end) {
            strcpy(ctx->src_id, ctx->node_id);
        } else {
            key_copy(ctx->src_id, key);
        }
        // if (ctx->ring_id == 0)
        printf("[%s] %s %s RING(%u): %s\n", ctx->src_id, msg == KV_ETCD_MSG_PUT ? "PUT" : "DEL", state_str[ctx->state], ctx->ring_id, ctx->node_id);
        if (msg == KV_ETCD_MSG_PUT) {
            assert(val_len == sizeof(ctx->vid));
            kv_memcpy(&ctx->vid, val, val_len);
        }
        ctx->msg_type = msg;
        kv_app_send_without_token(self->thread_id, on_ring_change, ctx);
    }
}
kv_rdma_handle kv_ring_init(char *etcd_ip, char *etcd_port, uint32_t thread_num, kv_ring_cb server_online_cb, void *arg) {
    struct kv_ring *self = &g_ring;
    self->server_online_cb = server_online_cb;
    self->arg = arg;
    self->nodes = NULL;
    self->rings = NULL;
    self->dqs = NULL;
    STAILQ_INIT(&self->conn_q);
    self->thread_id = kv_app_get_thread_index();
    self->thread_num = thread_num;
    kv_rdma_init(&self->h, thread_num);
    kvEtcdInit(etcd_ip, etcd_port, msg_handler);
    self->conn_q_poller = kv_app_poller_register(kv_conn_q_poller, self, 100000);
    return self->h;
}

// --- copy ---

void kv_ring_register_copy_cb(kv_ring_copy_cb copy_cb, void *cb_arg) {
    struct kv_ring *self = &g_ring;
    self->copy_cb = copy_cb;
    self->copy_cb_arg = cb_arg;
}
void kv_ring_stop_copy(struct kv_ring_copy_info *info) {
    struct kv_ring *self = &g_ring;
    struct copy_range_ctx *ctx = (struct copy_range_ctx *)info;
    TAILQ_INSERT_TAIL(&ctx->ring->copy_ranges, ctx, entry);
    if (--ctx->ring->range_size) return;
    uint32_t ring_id = ctx->ring - self->rings[0], type = ctx->invalid->state == VID_JOINING ? 0 : 2;
    static uint8_t key[MAX_ETCD_KEY_LEN];
    sprintf(key, "/rings/%u/%u/%s/%s/", ring_id, type, ctx->invalid->node->node_id, self->local_id);
    kvEtcdDel(key);
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
    struct vnode_chain *chain = NULL;
    struct forward_ctx *ctx = NULL;
    if (self->is_server_exiting) goto send_nak;
    chain = get_chain(KV_MSG_KEY(msg));
    if (chain == NULL) goto send_nak;
    ctx = kv_malloc(sizeof(*ctx));
    ctx->ring_version = self->rings_version[get_ring_id(KV_MSG_KEY(msg), self->log_ring_num)] + chain->ring->version;
    ctx->node = NULL;
    if (msg->type == KV_MSG_SET || msg->type == KV_MSG_DEL) {
        struct vid_entry *local, *next = NULL;
        if (msg->hop == chain->rpl_num + 1) {
            local = chain->copy;
        } else if (msg->hop < chain->rpl_num + 1) {
            local = chain->vids[msg->hop - 1];
        } else {
            goto send_nak;
        }
        if (local == NULL || !local->node->is_local) goto send_nak;

        uint32_t vnode_type = KV_RING_VNODE;
        if (msg->hop == chain->rpl_num) vnode_type = KV_RING_TAIL;
        if (msg->hop == chain->rpl_num + 1) vnode_type = KV_RING_COPY;
        if (msg->hop < chain->rpl_num) {
            next = chain->vids[msg->hop];
        } else if (msg->hop == chain->rpl_num && chain->copy) {
            next = chain->copy;
        }
        if (next) {
            ctx->node = next->node;
            msg->hop++;
            next->node->req_cnt++;
        }
        ctx->ring_version->counter++;
        self->req_handler(req_h, req, ctx, ctx->node != NULL, local->vid.ds_id, vnode_type, arg);
        kv_free(chain);
        return;
    } else if (msg->type == KV_MSG_GET) {
        if (msg->hop == 1) {
            uint32_t i = 0;
            for (; i < chain->rpl_num; i++)
                if (chain->vids[i]->node->is_local) break;
            if (i == chain->rpl_num) goto send_nak;
            struct vid_entry *tail = chain->vids[chain->rpl_num - 1];
            uint32_t vnode_type = KV_RING_TAIL;
            if (!tail->node->is_local) {
                ctx->node = tail->node;
                msg->hop++;
                tail->node->req_cnt++;
                vnode_type = KV_RING_VNODE;
            }
            ctx->ring_version->counter++;
            self->req_handler(req_h, req, ctx, ctx->node != NULL, chain->vids[i]->vid.ds_id, vnode_type, arg);
            kv_free(chain);
            return;
        } else if (msg->hop == 2) {
            struct vid_entry *tail = chain->vids[chain->rpl_num - 1];
            if (!tail->node->is_local) goto send_nak;
            ctx->ring_version->counter++;
            self->req_handler(req_h, req, ctx, ctx->node != NULL, tail->vid.ds_id, KV_RING_TAIL, arg);
            kv_free(chain);
            return;
        }
    }
    assert(false);
send_nak:
    if (chain) kv_free(chain);
    if (ctx) kv_free(ctx);
    msg->type = KV_MSG_OUTDATED;
    msg->value_len = 0;
    kv_rdma_make_resp(req_h, (uint8_t *)msg, KV_MSG_SIZE(msg));
}

void kv_ring_server_init(char *local_ip, char *local_port, uint32_t ring_num, uint32_t vid_per_ssd, uint32_t ds_num, uint32_t rpl_num,
                         uint32_t log_bkt_num, uint32_t con_req_num, uint32_t max_msg_sz, kv_ring_req_handler handler, void *arg,
                         kv_rdma_server_init_cb cb, void *cb_arg) {
    assert(ds_num != 0);
    assert(vid_per_ssd * ds_num <= ring_num);
    assert(local_ip && local_port);
    struct kv_ring *self = &g_ring;

    uint32_t log_ring_num = 0;
    while ((ring_num - 1) >> log_ring_num) log_ring_num++;
    ring_init(log_ring_num);
    ring_num = 1U << log_ring_num;

    self->req_handler = handler;
    kv_rdma_listen(self->h, local_ip, local_port, con_req_num, max_msg_sz, rdma_req_handler_wrapper, arg, cb, cb_arg);  // TODO: change this
    sprintf(self->local_id, "%s:%s", local_ip, local_port);
    static char key[MAX_ETCD_KEY_LEN];
    sprintf(key, "/nodes/%s/", self->local_id);
    self->node_lease = kvEtcdLeaseCreate(1, true);
    struct kv_etcd_node etcd_node = {.ds_num = ds_num, .log_ring_num = log_ring_num, .log_bkt_num = log_bkt_num, .rpl_num = rpl_num};
    kvEtcdPut(key, &etcd_node, sizeof(etcd_node), &self->node_lease);

    struct vid_ring_stat stats[ring_num];
    for (size_t i = 0; i < ring_num; i++) stats[i] = (struct vid_ring_stat){i, ring_size(self->rings[0] + i)};
    qsort(stats, ring_num, sizeof(struct vid_ring_stat), vid_ring_stat_cmp);
    self->vid_lease = kvEtcdLeaseCreate(5, true);  // vid lease ttl must larger than node lease ttl
    uint64_t init_lease = kvEtcdLeaseCreate(5, false);

    uint32_t ds_id = 0;
    for (size_t i = 0; i < vid_per_ssd * ds_num; i++) {
        struct kv_etcd_vid vid = {.ds_id = ds_id};
        // random_vid(vid.vid);
        hash_vid(vid.vid, local_ip, local_port, i);
        set_ring_id(vid.vid, log_ring_num, stats[i].index);
        sprintf(key, "/rings/%u/0/%s/", stats[i].index, self->local_id);  // joining
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
        kv_free(self->rings_version);
    }
    if (self->dqs) {
        for (size_t i = 0; i < self->thread_num; i++) kv_app_poller_unregister(&self->dq_pollers[i]);
        kv_free(self->dqs);
        kv_free(self->dq_pollers);
    }
}