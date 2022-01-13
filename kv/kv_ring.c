#include "kv_ring.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>

#include "kv_app.h"
#include "kv_etcd.h"
#include "memery_operation.h"
#include "pthread.h"
#include "uthash.h"

#define CIRCLEQ_FOREACH_SAFE(var, head, field, tvar) \
    for ((var) = CIRCLEQ_FIRST(head); (var) != (void *)(head) && ((tvar) = CIRCLEQ_NEXT(var, field), 1); (var) = (tvar))

struct kv_node {
    struct kv_node_info *info;
    connection_handle conn;
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

struct kv_ring {
    kv_rdma_handle h;
    uint32_t thread_id;
    char *local_ip;
    char *local_port;
    pthread_rwlock_t lock;
    struct kv_node *nodes;
    struct vid_ring *rings;
    pthread_mutex_t rings_lock;
    STAILQ_HEAD(, kv_node) conn_q;
    // ssd_status
    kv_ring_cb ready_cb;
    void *arg;
} g_ring;

static void random_vid(char *vid) {
    for (size_t i = 0; i < KV_VID_LEN; i++) vid[i] = random() & 0xFF;
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
    uint64_t vid64 = get_vid_64(vid);
    struct vid_entry *x = NULL, *next;
    CIRCLEQ_FOREACH(x, ring, entry) {
        next = CIRCLEQ_LOOP_NEXT(ring, x, entry);
        if (next == x) break;
        if (get_vid_64(next->vid->vid) - vid64 > get_vid_64(x->vid->vid) - vid64) break;
    }
    return x == (void *)(ring) ? NULL : x;
}
static void vid_ring_init(uint32_t vid_num) {
    struct kv_ring *self = &g_ring;
    pthread_mutex_lock(&self->rings_lock);
    if (self->rings == NULL) {
        self->rings = kv_calloc(vid_num, sizeof(struct vid_ring));
        for (size_t i = 0; i < vid_num; i++) CIRCLEQ_INIT(self->rings + i);
    }
    pthread_mutex_unlock(&self->rings_lock);
}

void kv_ring_dispatch(char *key, connection_handle *h, uint32_t *ssd_id) {
    struct kv_ring *self = &g_ring;
    pthread_rwlock_rdlock(&self->lock);
    if (self->nodes == NULL) {
        fprintf(stderr, "no available server!\n");
        exit(-1);
    }
    uint32_t index = get_vid_part(key, self->nodes->info->vid_num);
    struct vid_entry *entry = find_vid_entry(self->rings + index, key);
    pthread_rwlock_unlock(&self->lock);
    if (entry == NULL) {
        fprintf(stderr, "no available server for this partition!\n");
        exit(-1);
    }
    *h = entry->node->conn;
    *ssd_id = entry->vid->ssd_id;
}
static void rdma_disconnect_cb(void *arg) {
    struct kv_ring *self = &g_ring;
    struct kv_node *node = arg;
    if (node->is_disconnecting) {
        free(node->info);  // info is allocated by libkv_etcd
        kv_free(node);
    } else {
        node->is_connected=false;
        STAILQ_INSERT_TAIL(&self->conn_q, node, next);
    }
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
    printf("connected to node %s:%s.\n", node->info->rdma_ip, node->info->rdma_port);
    node->is_connected = true;
    node->conn = h;
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

static void node_handler(void *arg) {
    struct kv_node_info *info = arg;
    struct kv_ring *self = &g_ring;
    vid_ring_init(info->vid_num);
    if (info->msg_type == KV_NODE_INFO_DELETE) {
        struct kv_node *node = NULL;
        pthread_rwlock_wrlock(&self->lock);
        HASH_FIND(hh, self->nodes, info->rdma_ip, 24, node);
        assert(node);
        for (size_t i = 0; i < info->vid_num; i++) {  // remove vids from ring
            struct vid_entry *x = NULL, *tmp;
            CIRCLEQ_FOREACH_SAFE(x, self->rings + i, entry, tmp) {
                if (x->node == node) CIRCLEQ_REMOVE(self->rings + i, x, entry);
            }
        }
        HASH_DEL(self->nodes, node);
        pthread_rwlock_unlock(&self->lock);
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
        pthread_rwlock_wrlock(&self->lock);
        // node may already exist?
        HASH_ADD(hh, self->nodes, info->rdma_ip, 24, node);  // ip & port as key
        for (size_t i = 0; i < info->vid_num; i++) {         // add vids to ring
            struct vid_entry *entry = kv_malloc(sizeof(struct vid_entry)), *x;
            *entry = (struct vid_entry){info->vids + i, node};
            x = find_vid_entry(self->rings + i, info->vids[i].vid);
            if (x) {
                CIRCLEQ_INSERT_BEFORE(self->rings + i, x, entry, entry);
            } else {
                CIRCLEQ_INSERT_HEAD(self->rings + i, entry, entry);
            }
        }
        pthread_rwlock_unlock(&self->lock);
        if (!node->is_local) {
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

void kv_ring_init(char *etcd_ip, char *etcd_port, uint32_t thread_num, kv_ring_cb ready_cb, void *arg) {
    struct kv_ring *self = &g_ring;
    self->ready_cb = ready_cb;
    self->arg = arg;
    self->nodes = NULL;
    self->rings = NULL;
    self->thread_id = kv_app_get_thread_index();
    pthread_mutex_init(&self->rings_lock, NULL);
    kv_rdma_init(&self->h, thread_num);
    kvEtcdInit(etcd_ip, etcd_port, _node_handler, NULL);
    STAILQ_INIT(&self->conn_q);
    kv_app_poller_register(kv_conn_q_poller, self, 1000000);
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

void kv_ring_server_init(char *local_ip, char *local_port, uint32_t vid_num, uint32_t vid_per_ssd, uint32_t ssd_num,
                         uint32_t con_req_num, uint32_t max_msg_sz, kv_rdma_req_handler handler, void *arg) {
    assert(ssd_num != 0);
    assert(vid_per_ssd * ssd_num <= vid_num);
    assert(local_ip && local_port);
    struct kv_ring *self = &g_ring;
    kv_rdma_listen(self->h, local_ip, local_port, con_req_num, max_msg_sz, handler, arg);
    self->local_ip = local_ip;
    self->local_port = local_port;
    vid_ring_init(vid_num);
    struct kv_node_info *info = kv_node_info_alloc(local_ip, local_port, vid_num);
    struct vid_ring_stat *stats = kv_calloc(vid_num, sizeof(struct vid_ring_stat));
    pthread_rwlock_rdlock(&self->lock);
    for (size_t i = 0; i < vid_num; i++) stats[i] = (struct vid_ring_stat){i, ring_size(self->rings + i)};
    pthread_rwlock_unlock(&self->lock);
    qsort(stats, vid_num, sizeof(struct vid_ring_stat), vid_ring_stat_cmp);
    uint32_t ssd_id = 0;
    for (size_t i = 0; i < vid_per_ssd * ssd_num; i++) {
        info->vids[stats[i].index].ssd_id = ssd_id;
        random_vid(info->vids[stats[i].index].vid);
        ssd_id = (ssd_id + 1) % ssd_num;
    }
    kv_free(stats);
    kvEtcdCreateNode(info, 1);
    free(info);
    kv_app_poller_register(kv_etcd_poller, NULL, 300000);
}

void kv_ring_fini(void) {
    struct kv_ring *self = &g_ring;
    kvEtcdFini();
    // kv_rdma_fini(self->h);
    if (self->rings) kv_free(self->rings);
}