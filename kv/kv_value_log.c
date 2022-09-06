#include "kv_value_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>
#include <sys/uio.h>

#include "kv_memory.h"
#include "utils/uthash.h"
static inline uint64_t align(struct kv_value_log *self, uint64_t size) {
    if (size & self->blk_mask) return (size >> self->blk_shift) + 1;
    return size >> self->blk_shift;
}
static inline uint64_t dword_align(uint64_t size) { return size & 0x3 ? (size & ~0x3) + 0x4 : size; }
// --- read ---
struct read_ctx {
    uint8_t *value;
    kv_circular_log_io_cb cb;
    void *cb_arg;
    uint8_t *buf;
    uint16_t buf_offset, len_in_buf;
};

static void read_cb(bool success, void *cb_arg) {
    struct read_ctx *ctx = (struct read_ctx *)cb_arg;
    kv_memcpy(ctx->value, ctx->buf + ctx->buf_offset, ctx->len_in_buf);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_storage_free(ctx->buf);
    kv_free(ctx);
}

void kv_value_log_read(struct kv_value_log *self, uint64_t offset, uint8_t *value, uint32_t value_length,
                       kv_circular_log_io_cb cb, void *cb_arg) {
    assert((offset & 0x3) == 0);
    if (offset & self->blk_mask) {
        struct read_ctx *ctx = kv_malloc(sizeof(struct read_ctx));
        ctx->value = value;
        ctx->cb = cb;
        ctx->cb_arg = cb_arg;
        ctx->buf_offset = offset & self->blk_mask;
        ctx->len_in_buf = self->log.storage->block_size - ctx->buf_offset;
        ctx->buf = kv_storage_blk_alloc(self->log.storage, 1);
        if (value_length > ctx->len_in_buf) {
            struct iovec iov[2] = {{ctx->buf, 1}, {value + ctx->len_in_buf, align(self, value_length - ctx->len_in_buf)}};
            kv_circular_log_readv(&self->log, offset >> self->blk_shift, iov, 2, read_cb, ctx);
        } else {
            ctx->len_in_buf = value_length;
            kv_circular_log_read(&self->log, offset >> self->blk_shift, ctx->buf, 1, read_cb, ctx);
        }
    } else
        kv_circular_log_read(&self->log, offset >> self->blk_shift, value, align(self, value_length), cb, cb_arg);
}

// --- bucket ids log ---
#define BUCKET_ID_DUMP_BLKS (128u)
#define BUCKET_ID_PER_BLK (85u)
struct uint48_t {
    uint64_t val : 48;
} __attribute__((packed));

struct bucket_ids_block {
    struct uint48_t ids[BUCKET_ID_PER_BLK];
    uint16_t reserved;
};

struct bucket_ids_ctx {
    struct kv_circular_log log;
    struct bucket_ids_block *blk_buf[2];
    uint32_t blk_index;
    bool is_dumping;
};

static inline uint64_t offset_to_block(uint64_t val_offset) {
    return (val_offset >> KV_VALUE_LOG_UNIT_SHIFT) / BUCKET_ID_PER_BLK;
}
static inline struct uint48_t *get_bucket_id_from_blk(struct bucket_ids_block *blk, uint64_t val_offset) {
    return &blk->ids[(val_offset >> KV_VALUE_LOG_UNIT_SHIFT) % BUCKET_ID_PER_BLK];
}

static struct uint48_t *get_bucket_id(struct kv_value_log *self, uint64_t val_offset) {
    struct bucket_ids_ctx *ctx = self->bucket_id_log;
    struct bucket_ids_block *blk;
    kv_circular_log_fetch_one(&ctx->log, offset_to_block(val_offset), (void **)&blk);
    return get_bucket_id_from_blk(blk, val_offset);
}

static void dump_bucket_id_cb(bool success, void *arg) {
    if (!success) {
        fprintf(stderr, "value log: dumping bucket ids has failed.");
        exit(-1);
    }
    struct bucket_ids_ctx *ctx = arg;
    ctx->is_dumping = false;
}

static void append_bucket_id(struct kv_value_log *self, uint64_t val_offset, uint64_t bucket_id) {
    struct bucket_ids_ctx *ctx = self->bucket_id_log;
    uint64_t blk_i = (ctx->log.size - ctx->log.tail + offset_to_block(val_offset)) % ctx->log.size;
    assert(blk_i < 2 * BUCKET_ID_DUMP_BLKS);
    if (blk_i >= BUCKET_ID_DUMP_BLKS) {
        if (ctx->is_dumping) {
            fprintf(stderr, "value log: dumping bucket ids is too slow.");
            exit(-1);
        }
        ctx->is_dumping = true;
        kv_circular_log_append(&ctx->log, ctx->blk_buf[ctx->blk_index], BUCKET_ID_DUMP_BLKS, dump_bucket_id_cb, ctx);
        ctx->blk_index = !ctx->blk_index;
        kv_memset(ctx->blk_buf[ctx->blk_index], 0xFF, BUCKET_ID_DUMP_BLKS << self->blk_shift);
        blk_i -= BUCKET_ID_DUMP_BLKS;
    }
    get_bucket_id_from_blk(&ctx->blk_buf[ctx->blk_index][blk_i], val_offset)->val = bucket_id;
}

static uint64_t on_val_log_head_move(struct kv_value_log *self) {
    struct bucket_ids_ctx *ctx = self->bucket_id_log;
    uint64_t max_blk = (self->log.size + self->compact_head - self->log.head) % self->log.size;
    uint64_t val_offset;
    for (uint64_t i = 0; i <= max_blk << self->blk_shift; i += KV_VALUE_LOG_UNIT_SIZE) {
        val_offset = ((self->log.head << self->blk_shift) + i) % (self->log.size << self->blk_shift);
        if (get_bucket_id(self, val_offset)->val != KV_BUCKET_ID_EMPTY) break;
    }
    val_offset &= ~self->blk_mask;
    uint64_t offset = (ctx->log.size + offset_to_block(val_offset) - ctx->log.head) % ctx->log.size;
    kv_circular_log_move_head(&ctx->log, offset);
    return val_offset;
}

static void bucket_id_log_init(struct kv_value_log *self) {
    struct bucket_ids_ctx *ctx = kv_malloc(sizeof(*ctx));
    assert(sizeof(struct bucket_ids_block) == 1 << self->blk_shift);
    self->id_log_size = offset_to_block(self->log.size << self->blk_shift) + 1;
    uint64_t base = self->log.base + self->log.size;
    kv_circular_log_init(&ctx->log, self->log.storage, base, self->id_log_size, 0, 0, BUCKET_ID_DUMP_BLKS * 2, BUCKET_ID_DUMP_BLKS / 8);
    ctx->blk_buf[0] = kv_storage_blk_alloc(ctx->log.storage, BUCKET_ID_DUMP_BLKS);
    ctx->blk_buf[1] = kv_storage_blk_alloc(ctx->log.storage, BUCKET_ID_DUMP_BLKS);
    ctx->blk_index = 0;
    ctx->is_dumping = false;
    kv_memset(ctx->blk_buf[ctx->blk_index], 0xFF, BUCKET_ID_DUMP_BLKS << self->blk_shift);
    self->bucket_id_log = ctx;
}

static void bucket_id_log_fini(struct kv_value_log *self) {
    struct bucket_ids_ctx *ctx = self->bucket_id_log;
    kv_storage_free(ctx->blk_buf[0]);
    kv_storage_free(ctx->blk_buf[1]);
    kv_circular_log_fini(&ctx->log);
    kv_free(ctx);
}

// --- compaction ---
#define COMPACTION_CONCURRENCY 4U
#define COMPACTION_LENGTH 256U

struct item_list_entry {
    struct kv_value_log *self;
    struct kv_bucket_segment *seg;
    uint64_t value_offset;
    struct kv_item *item;
    TAILQ_ENTRY(item_list_entry)
    entry;
};
TAILQ_HEAD(item_list, item_list_entry);

struct compact_ctx {
    struct kv_value_log *self;
    struct kv_bucket_segments segments;
    struct item_list items;
    uint32_t iocnt;
};
#define TAILQ_FOREACH_SAFE(var, head, field, tvar) \
    for ((var) = TAILQ_FIRST((head)); (var) && ((tvar) = TAILQ_NEXT((var), field), 1); (var) = (tvar))

static void compact_move_head(struct kv_value_log *self) {
    uint64_t blk = on_val_log_head_move(self) >> self->blk_shift;
    uint64_t offset = (self->log.size + blk - self->log.head) % self->log.size;
    kv_circular_log_move_head(&self->log, offset);
}

static void compact_write(bool success, void *arg) {
    struct compact_ctx *ctx = arg;
    struct kv_value_log *self = ctx->self;
    if (success == false) {
        fprintf(stderr, "value log compaction failed.\n");
        exit(-1);
    }
    if (--ctx->iocnt) return;

    struct item_list_entry *entry, *tmp;
    TAILQ_FOREACH_SAFE(entry, &ctx->items, entry, tmp) {
        get_bucket_id(self, entry->value_offset)->val = KV_BUCKET_ID_EMPTY;
        TAILQ_REMOVE(&ctx->items, entry, entry);
        kv_free(entry);
    }

    struct kv_bucket_segment *seg;
    TAILQ_FOREACH(seg, &ctx->segments, entry) {
        kv_bucket_seg_commit(self->bucket_log, seg);
    }
    kv_bucket_unlock(self->bucket_log, &ctx->segments);
    while ((seg = TAILQ_FIRST(&ctx->segments)) != NULL) {
        TAILQ_REMOVE(&ctx->segments, seg, entry);
        kv_free(seg);
    }

    compact_move_head(self);
    kv_free(ctx);
}

static inline uint8_t *last_buf(struct kv_value_log *self, struct iovec *iov, uint32_t iocnt) {
    iov = iov + iocnt - 1;
    return (uint8_t *)iov->iov_base + ((iov->iov_len - 1) << self->blk_shift);
}

static void append_val_iov(struct kv_value_log *self, struct iovec *iov, uint32_t *cnt, uint64_t tail, uint8_t *src,
                           uint32_t len) {
    tail = tail & self->blk_mask;
    if (tail) {
        uint8_t *dst = last_buf(self, iov, *cnt) + tail;
        uint64_t n = (1 << self->blk_shift) - tail;
        // dst and src may overlap
        if (len <= n) {
            kv_memmove(dst, src, len);
        } else {
            kv_memmove(dst, src, n);
            iov[*cnt].iov_len = align(self, len - n);
            iov[(*cnt)++].iov_base = src + n;
        }
    } else {
        iov[*cnt].iov_len = align(self, len);
        iov[(*cnt)++].iov_base = src;
    }
}

static void compact_lock_cb(void *arg) {
    struct compact_ctx *ctx = arg;
    struct kv_value_log *self = ctx->self;

    // find all the values that need to be moved.
    uint64_t items_num = 0;
    struct item_list_entry *entry, *tmp0;
    TAILQ_FOREACH_SAFE(entry, &ctx->items, entry, tmp0) {
        items_num++;
        struct kv_bucket_chain_entry *ce;
        TAILQ_FOREACH(ce, &entry->seg->chain, entry) {
            for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket)
                for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item) {
                    if (KV_EMPTY_ITEM(item)) continue;
                    if ((item->value_offset & ~KV_VALUE_LOG_UNIT_MASK) == entry->value_offset) {
                        entry->item = item;
                        goto next_item;
                    }
                }
        }
        // outdated values
        items_num--;
        get_bucket_id(self, entry->value_offset)->val = KV_BUCKET_ID_EMPTY;
        TAILQ_REMOVE(&ctx->items, entry, entry);
        kv_free(entry);
    next_item:;
    }
    // unlock the segments having no item_entry reference.
    struct kv_bucket_segments unlock_segs;
    TAILQ_INIT(&unlock_segs);
    struct kv_bucket_segment *seg, *tmp1;
    TAILQ_FOREACH_SAFE(seg, &ctx->segments, entry, tmp1) {
        TAILQ_FOREACH(entry, &ctx->items, entry) {
            if (entry->seg == seg) {
                seg->dirty = true;
                goto next_seg;
            }
        }
        TAILQ_REMOVE(&ctx->segments, seg, entry);
        TAILQ_INSERT_TAIL(&unlock_segs, seg, entry);
    next_seg:;
    }
    if (!TAILQ_EMPTY(&unlock_segs)) kv_bucket_unlock(self->bucket_log, &unlock_segs);
    while ((seg = TAILQ_FIRST(&unlock_segs)) != NULL) {
        TAILQ_REMOVE(&unlock_segs, seg, entry);
        kv_free(seg);
    }

    if (TAILQ_EMPTY(&ctx->segments)) {
        compact_move_head(self);
        kv_free(ctx);
        return;
    }
    // value compaction
    struct iovec iov[2], val_iov[items_num + 2];
    uint64_t tail = 0;
    uint32_t iovcnt = 0;
    TAILQ_FOREACH(entry, &ctx->items, entry) {
        struct kv_item *item = entry->item;
        uint64_t val_blk = item->value_offset >> self->blk_shift;
        uint64_t val_end = align(self, item->value_offset + item->value_length);
        if (!kv_circular_log_is_fetchable(&self->log, val_end)) {
            fprintf(stderr, "value_log_compaction: value log prefetch is too slow.\n");
            exit(-1);
        }
        kv_circular_log_fetch(&self->log, val_blk, val_end - val_blk, iov);

        if (iov[1].iov_len) {
            uint32_t offset_in_buf = item->value_offset & self->blk_mask;
            uint32_t len = (iov->iov_len << self->blk_shift) - offset_in_buf;
            append_val_iov(self, val_iov, &iovcnt, tail, (uint8_t *)iov->iov_base + offset_in_buf, len);
            append_val_iov(self, val_iov, &iovcnt, tail, iov[1].iov_base, item->value_length - len);
        } else {
            uint8_t *src = (uint8_t *)iov->iov_base + (item->value_offset & self->blk_mask);
            append_val_iov(self, val_iov, &iovcnt, tail, src, item->value_length);
        }
        item->value_offset = (kv_value_log_offset(self) + tail) % (self->log.size << self->blk_shift);
        append_bucket_id(self, item->value_offset, entry->seg->bucket_id);

        uint64_t next_tail = tail + dword_align(item->value_length);
        if (tail >> KV_VALUE_LOG_UNIT_SHIFT == next_tail >> KV_VALUE_LOG_UNIT_SHIFT)
            tail = (tail & ~KV_VALUE_LOG_UNIT_MASK) + KV_VALUE_LOG_UNIT_SIZE;
        else
            tail = next_tail;
    }
    ctx->iocnt = 2;
    kv_bucket_seg_put_bulk(self->bucket_log, &ctx->segments, compact_write, ctx);
    kv_circular_log_appendv(&self->log, val_iov, iovcnt, compact_write, ctx);
}

static void compact(struct kv_value_log *self) {
    if (!self->bucket_log || kv_circular_log_empty_space(&self->log) >= COMPACTION_LENGTH * COMPACTION_CONCURRENCY * 8) return;
    if ((self->log.size - self->log.head + self->compact_head) % self->log.size > COMPACTION_LENGTH * COMPACTION_CONCURRENCY) return;
    if (self->is_compaction_started == false) {
        puts("kv_value_log: the compaction has started.");
        self->is_compaction_started = true;
    }
    struct compact_ctx *ctx = kv_malloc(sizeof(struct compact_ctx));
    ctx->self = self;
    TAILQ_INIT(&ctx->segments);
    TAILQ_INIT(&ctx->items);
    for (size_t i = 0; i < (COMPACTION_LENGTH << self->blk_shift); i += KV_VALUE_LOG_UNIT_SIZE) {
        uint64_t val_offset = ((self->compact_head << self->blk_shift) + i) % (self->log.size << self->blk_shift);
        uint64_t bucket_id = get_bucket_id(self, val_offset)->val;
        if (bucket_id == KV_BUCKET_ID_EMPTY) continue;
        struct kv_bucket_segment *seg = NULL;
        TAILQ_FOREACH(seg, &ctx->segments, entry) {
            if (seg->bucket_id == bucket_id) break;
        }
        if (seg == NULL) {
            seg = kv_malloc(sizeof(*seg));
            kv_bucket_seg_init(seg, bucket_id);
            TAILQ_INSERT_TAIL(&ctx->segments, seg, entry);
        }

        struct item_list_entry *entry = kv_malloc(sizeof(*entry));
        *entry = (struct item_list_entry){self, seg, val_offset, NULL};
        TAILQ_INSERT_TAIL(&ctx->items, entry, entry);
    }
    self->compact_head = (self->compact_head + COMPACTION_LENGTH) % self->log.size;
    if (TAILQ_EMPTY(&ctx->segments)) {
        compact_move_head(self);
        kv_free(ctx);
        return;
    }
    kv_bucket_lock(self->bucket_log, &ctx->segments, compact_lock_cb, ctx);
}

//--- write ---
void kv_value_log_write(struct kv_value_log *self, uint64_t bucket_id, uint8_t *value, uint32_t value_length,
                        kv_circular_log_io_cb cb, void *cb_arg) {
    append_bucket_id(self, kv_value_log_offset(self), bucket_id);
    kv_circular_log_append(&self->log, value, align(self, value_length), cb, cb_arg);
    compact(self);
}

// --- init & fini ---
void kv_value_log_init(struct kv_value_log *self, struct kv_storage *storage, struct kv_bucket_log *bucket_log, uint64_t base,
                       uint64_t size, uint32_t index_buf_len) {
    kv_memset(self, 0, sizeof(struct kv_value_log));
    for (self->blk_shift = 0; !((storage->block_size >> self->blk_shift) & 1); ++self->blk_shift)
        ;
    assert(storage->block_size == 1U << self->blk_shift);
    self->blk_mask = storage->block_size - 1;
    assert(!(base & self->blk_mask || size & self->blk_mask));
    self->bucket_log = bucket_log;
    kv_circular_log_init(&self->log, storage, base, size >> self->blk_shift, 0, 0,
                         COMPACTION_LENGTH * COMPACTION_CONCURRENCY * 8, 256);
    bucket_id_log_init(self);
}

void kv_value_log_fini(struct kv_value_log *self) {
    bucket_id_log_fini(self);
    kv_circular_log_fini(&self->log);
}
