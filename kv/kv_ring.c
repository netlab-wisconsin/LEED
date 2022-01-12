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
    char *local_ip;
    char *local_port;
    pthread_rwlock_t lock;
    struct kv_node *nodes;
    struct vid_ring *rings;
    pthread_mutex_t rings_lock;
    // ssd_status
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

static void node_handler(struct kv_node_info *info) {
    struct kv_ring *self = &g_ring;
    vid_ring_init(info->vid_num);
    bool is_local = self->local_ip && strcmp(self->local_ip, info->rdma_ip) && strcmp(self->local_port, info->rdma_port);
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
        kv_free(node);
        free(info);
        if (is_local) {
            fprintf(stderr, "kv_etcd keepalive timeout!\n");
            exit(-1);
        } else {
            // rdma disconnect
        }
    } else {
        struct kv_node *node = kv_malloc(sizeof(struct kv_node));
        node->info = info;
        pthread_rwlock_wrlock(&self->lock);
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
        if (!is_local) {
            // rdma connnect
        }
    }
}

static int kv_etcd_poller(void *arg) {
    kvEtcdKeepAlive();
    return 0;
}

void kv_ring_init(char *etcd_ip, char *etcd_port, uint32_t thread_num) {
    struct kv_ring *self = &g_ring;
    self->nodes = NULL;
    self->rings = NULL;
    pthread_mutex_init(&self->rings_lock, NULL);
    kvEtcdInit(etcd_ip, etcd_port, node_handler, NULL);
    kv_rdma_init(&self->h, thread_num);
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

void kv_ring_server_init(char *local_ip, char *local_port, uint32_t vid_num, uint32_t vid_per_ssd, uint32_t ssd_num) {
    assert(ssd_num != 0);
    assert(vid_per_ssd * ssd_num <= vid_num);
    assert(local_ip && local_port);
    struct kv_ring *self = &g_ring;
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
    // kv_rdma_listen
}

void kv_ring_fini(void) {
    struct kv_ring *self = &g_ring;
    kvEtcdFini();
    // kv_rdma_fini(self->h);
    if (self->rings) kv_free(self->rings);
}