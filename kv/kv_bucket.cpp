#include <cstdint>
#include <list>
#include <unordered_map>
#include <unordered_set>
extern "C" {
#include "kv_app.h"
#include "kv_bucket_log.h"
}

using namespace std;
// --- bucket meta ---
#define META_BLOCK_SHIFT (7u)
#define META_BLOCK_SIZE (1 << META_BLOCK_SHIFT)
#define META_BLOCK_MASK (META_BLOCK_SIZE - 1)
struct meta_block {
    struct kv_bucket_meta meta[META_BLOCK_SIZE];
    uint32_t non_empty_blks;
};
typedef unordered_map<uint64_t, meta_block> meta_map;
void kv_bucket_meta_init(struct kv_bucket_log *self) {
    self->meta = new meta_map();
}
void kv_bucket_meta_fini(struct kv_bucket_log *self) {
    delete (meta_map *)self->meta;
}

struct kv_bucket_meta kv_bucket_meta_get(struct kv_bucket_log *self, uint64_t bucket_id) {
    meta_map *meta = (meta_map *)self->meta;
    auto p = meta->find(bucket_id >> META_BLOCK_SHIFT);
    if (p == meta->end()) {
        return {0, 0};
    } else {
        return p->second.meta[bucket_id & META_BLOCK_MASK];
    }
}
static inline bool is_meta_empty(const struct kv_bucket_meta &data) {
    return data.chain_length == 0;
}

void kv_bucket_meta_put(struct kv_bucket_log *self, uint64_t bucket_id, struct kv_bucket_meta data) {
    meta_map *meta = (meta_map *)self->meta;
    auto p = meta->find(bucket_id >> META_BLOCK_SHIFT);
    if (p == meta->end()) {
        if (is_meta_empty(data)) return;
        (*meta)[bucket_id >> META_BLOCK_SHIFT] = {{}, 0};
        p = meta->find(bucket_id >> META_BLOCK_SHIFT);
    }
    bool empty = is_meta_empty(p->second.meta[bucket_id & META_BLOCK_MASK]);
    p->second.meta[bucket_id & META_BLOCK_MASK] = data;
    if (is_meta_empty(data)) {
        if (--p->second.non_empty_blks == 0) {
            meta->erase(p);
        }
    } else if (empty) {
        p->second.non_empty_blks++;
    }
}

// --- bucket lock ---
typedef unordered_set<uint64_t> lock_set_t;
struct lock_entry {
    lock_set_t *ids;
    kv_task_cb cb;
    void *cb_arg;
};

struct bucket_lock {
    lock_set_t locked;
    list<struct lock_entry> queue;
};
void kv_bucket_lock_set_add(kv_bucket_lock_set *lock_set, uint64_t bucket_id) {
    if (*lock_set == nullptr) {
        *lock_set = new lock_set_t();
    }
    ((lock_set_t *)*lock_set)->insert(bucket_id);
}

void kv_bucket_lock_set_del(kv_bucket_lock_set *lock_set, uint64_t bucket_id) {
    lock_set_t *p = (lock_set_t *)*lock_set;
    p->erase(bucket_id);
    if (p->empty()) {
        delete p;
        *lock_set = nullptr;
    }
}

static inline bool lockable(struct bucket_lock *ctx, lock_set_t *lock_set) {
    for (auto i = lock_set->begin(); i != lock_set->end(); ++i)
        if (ctx->locked.find(*i) != ctx->locked.end()) return false;
    return true;
}

void kv_bucket_lock(struct kv_bucket_log *self, kv_bucket_lock_set _lock_set, kv_task_cb cb, void *cb_arg) {
    struct bucket_lock *ctx = (struct bucket_lock *)self->bucket_lock;
    lock_set_t *lock_set = (lock_set_t *)_lock_set;
    if (lockable(ctx, lock_set)) {
        ctx->locked.insert(lock_set->begin(), lock_set->end());
        if (cb) kv_app_send(self->log.thread_index, cb, cb_arg);
    } else {
        ctx->queue.push_back({lock_set, cb, cb_arg});
    }
}
void kv_bucket_unlock(struct kv_bucket_log *self, kv_bucket_lock_set *_lock_set) {
    struct bucket_lock *ctx = (struct bucket_lock *)self->bucket_lock;
    lock_set_t *lock_set = (lock_set_t *)*_lock_set;
    if (lock_set == nullptr) return;
    for (auto i = lock_set->begin(); i != lock_set->end(); ++i)
        ctx->locked.erase(*i);
    for (auto i = lock_set->begin(); i != lock_set->end(); ++i)
        for (auto j = ctx->queue.begin(); j != ctx->queue.end(); ++j)
            if (j->ids->find(*i) != j->ids->end() && lockable(ctx, j->ids)) {
                ctx->locked.insert(j->ids->begin(), j->ids->end());
                if (j->cb) kv_app_send(self->log.thread_index, j->cb, j->cb_arg);
                ctx->queue.erase(j);
                break;
            }
    delete lock_set;
    *_lock_set = nullptr;
}

void kv_bucket_lock_init(struct kv_bucket_log *self) {
    self->bucket_lock = (void *)new bucket_lock();
}

void kv_bucket_lock_fini(struct kv_bucket_log *self) {
    delete (struct bucket_lock *)self->bucket_lock;
}