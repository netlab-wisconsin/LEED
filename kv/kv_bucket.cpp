#include <cassert>
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

struct lock_ctx {
    struct kv_bucket_segments *segs;
    uint32_t io_cnt;
    kv_task_cb cb;
    void *cb_arg;
};

struct lock_segment {
    struct kv_bucket_segment *seg;
    struct lock_ctx *ctx;
};

struct bucket_lock {
    unordered_set<uint64_t> locked;
    unordered_map<uint64_t, struct lock_segment> segments;
    list<struct lock_ctx *> queue;
};

static inline void segment_move(struct kv_bucket_segment *src, struct kv_bucket_segment *dst) {
    assert(dst->bucket_id == src->bucket_id);
    assert(TAILQ_EMPTY(&dst->chain) && dst->empty && !dst->dirty);
    assert(!src->empty && !src->dirty);
    struct kv_bucket_chain_entry *chain_entry;
    while ((chain_entry = TAILQ_FIRST(&src->chain)) != NULL) {
        TAILQ_REMOVE(&src->chain, chain_entry, entry);
        TAILQ_INSERT_TAIL(&dst->chain, chain_entry, entry);
    }
    src->empty = true;
    dst->empty = false;
}

static void lock_get_seg_cb(bool success, void *cb_arg) {
    if (success == false) {
        fprintf(stderr, "lock_get_seg failed.\n");
        exit(-1);
    }
    struct lock_segment *lock_seg = (struct lock_segment *)cb_arg;
    if (lock_seg->ctx == nullptr) return;
    struct lock_ctx *ctx = lock_seg->ctx;
    struct kv_bucket_segment *seg;
    TAILQ_FOREACH(seg, ctx->segs, entry) {
        if (seg->bucket_id == lock_seg->seg->bucket_id) {
            if (lock_seg->seg != seg) {
                segment_move(lock_seg->seg, seg);
                lock_seg->seg = seg;
            }
            break;
        }
    }
    lock_seg->ctx = nullptr;
    if (--ctx->io_cnt == 0) {
        if (ctx->cb) kv_app_send(kv_app_get_thread_index(), ctx->cb, ctx->cb_arg);
        delete ctx;
    }
}
static inline bool lockable(struct bucket_lock *lock, struct kv_bucket_segments *segs) {
    struct kv_bucket_segment *seg;
    TAILQ_FOREACH(seg, segs, entry) {
        if (lock->locked.find(seg->bucket_id) != lock->locked.end()) return false;
    }
    return true;
}
static bool try_lock(struct kv_bucket_log *self, struct lock_ctx *ctx) {
    struct bucket_lock *lock = (struct bucket_lock *)self->bucket_lock;
    if (!lockable(lock, ctx->segs)) return false;
    struct kv_bucket_segment *seg;
    TAILQ_FOREACH(seg, ctx->segs, entry) {
        lock->locked.insert(seg->bucket_id);
        ctx->io_cnt++;
    }
    TAILQ_FOREACH(seg, ctx->segs, entry) {
        auto p = lock->segments.find(seg->bucket_id);
        if (p != lock->segments.end()) {
            assert(p->second.ctx == nullptr);
            p->second.ctx = ctx;
            if (p->second.seg->empty == false) {
                lock_get_seg_cb(true, &lock->segments[seg->bucket_id]);
            }
        } else {
            lock->segments[seg->bucket_id] = {seg, ctx};
            kv_bucket_seg_get(self, seg, false, lock_get_seg_cb, &lock->segments[seg->bucket_id]);
        }
    }
    return true;
}

void kv_bucket_lock(struct kv_bucket_log *self, struct kv_bucket_segments *segs, kv_task_cb cb, void *cb_arg) {
    struct bucket_lock *lock = (struct bucket_lock *)self->bucket_lock;
    struct lock_ctx *ctx = new lock_ctx{.segs = segs, .io_cnt = 0, .cb = cb, .cb_arg = cb_arg};
    if (try_lock(self, ctx) == false) {
        lock->queue.push_back(ctx);
        struct kv_bucket_segment *seg;
        TAILQ_FOREACH(seg, segs, entry) {
            if (lock->segments.find(seg->bucket_id) == lock->segments.end()) {
                // prefetch the bucket segment
                lock->segments[seg->bucket_id] = {seg, nullptr};
                kv_bucket_seg_get(self, seg, false, lock_get_seg_cb, &lock->segments[seg->bucket_id]);
            }
        }
    }
}
void kv_bucket_unlock(struct kv_bucket_log *self, struct kv_bucket_segments *segs) {
    struct bucket_lock *lock = (struct bucket_lock *)self->bucket_lock;
    struct kv_bucket_segment *seg, *i;

    TAILQ_FOREACH(seg, segs, entry) {
        auto &lock_seg = lock->segments[seg->bucket_id];
        assert(lock_seg.seg == seg);
        lock->locked.erase(seg->bucket_id);
        for (auto p = lock->queue.begin(); p != lock->queue.end(); ++p)
            TAILQ_FOREACH(i, (*p)->segs, entry) {
                if (i->bucket_id == seg->bucket_id && seg->dirty == false) {
                    segment_move(seg, i);
                    lock_seg.seg = i;
                    goto next_seg;
                }
            }
        lock->segments.erase(seg->bucket_id);
        kv_bucket_seg_cleanup(self, seg);
    next_seg:;
    }

    for (auto p = lock->queue.begin(); p != lock->queue.end();)
        if (try_lock(self, *p)) {
            p = lock->queue.erase(p);
        } else {
            ++p;
        }
}

void kv_bucket_lock_init(struct kv_bucket_log *self) {
    self->bucket_lock = (void *)new bucket_lock();
}

void kv_bucket_lock_fini(struct kv_bucket_log *self) {
    delete (struct bucket_lock *)self->bucket_lock;
}