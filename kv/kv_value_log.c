#include "kv_value_log.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>
#include <sys/uio.h>

#include "memery_operation.h"
#include "uthash.h"
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
    kv_storage_free(ctx->buf);
    if (ctx->cb) ctx->cb(success, cb_arg);
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
        struct iovec iov[2] = {{ctx->buf, 1}, {value + ctx->len_in_buf, align(self, value_length - ctx->len_in_buf)}};
        kv_circular_log_readv(&self->log, offset >> self->blk_shift, iov, 2, read_cb, ctx);
    } else
        kv_circular_log_read(&self->log, offset >> self->blk_shift, value, align(self, value_length), cb, cb_arg);
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

    uint64_t index_log_block_num = size / (KV_VALUE_LOG_UNIT_SIZE + sizeof(uint32_t)) / KV_INDEX_LOG_ENTRY_PER_BLOCK;
    kv_circular_log_init(&self->index_log, storage, base >> self->blk_shift, index_log_block_num, 0, 0, index_buf_len * 2,
                         index_buf_len / 8);
    uint64_t units_per_block = storage->block_size / KV_VALUE_LOG_UNIT_SIZE;
    uint64_t block_num = index_log_block_num * KV_INDEX_LOG_ENTRY_PER_BLOCK / units_per_block;

    self->bucket_log = bucket_log;
    self->index_buf_len = index_buf_len;
    self->index_buf = kv_storage_blk_alloc(storage, self->index_buf_len);
    kv_memset(self->index_buf, 0xFF, self->index_buf_len * storage->block_size);

    self->compact.index_blk_num = 256;  // TODO: change this
    self->compact.index_buf = kv_storage_blk_alloc(storage, self->compact.index_blk_num);
    self->compact.val_buf_len = self->compact.index_blk_num * KV_INDEX_LOG_ENTRY_PER_BLOCK / units_per_block;
    self->compact.val_buf = kv_storage_blk_alloc(storage, self->compact.val_buf_len);
    kv_circular_log_init(&self->log, storage, self->index_log.base + self->index_log.size, block_num, 0, 0,
                         self->compact.val_buf_len * 2, 256);
}

void kv_value_log_fini(struct kv_value_log *self) {
    kv_storage_free(self->index_buf);
    kv_storage_free(self->compact.val_buf);
    kv_circular_log_fini(&self->log);
}

// --- index log write ---
#define OFFSET_TO_BLOCK(offset) ((offset) >> (KV_INDEX_LOG_ENTRY_BIT + KV_VALUE_LOG_UNIT_SHIFT))
#define OFFSET_TO_INDEX(offset) (((offset) >> KV_VALUE_LOG_UNIT_SHIFT) & KV_INDEX_LOG_ENTRY_MASK)

struct index_dump_ctx {
    struct kv_value_log *self;
    uint32_t *index_buf;
    uint64_t offset;
};
static void index_dump_cb(bool success, void *arg) {
    struct index_dump_ctx *ctx = arg;
    struct kv_circular_log *index_log = &ctx->self->index_log;
    if (success) {
        kv_storage_free(ctx->index_buf);
        kv_free(ctx);
    } else {
        fprintf(stderr, "kv_index_log dump fail, retrying ...\n");
        kv_circular_log_write(index_log, ctx->offset, ctx->index_buf, ctx->self->index_buf_len, index_dump_cb, ctx);
    }
}

static void index_log_write(struct kv_value_log *self, uint64_t offset, uint32_t bucket_index) {
    struct kv_circular_log *index_log = &self->index_log;
    uint64_t blk_i = (index_log->size - index_log->tail + OFFSET_TO_BLOCK(offset)) % index_log->size;
    assert(blk_i < 2 * self->index_buf_len);
    uint64_t i = OFFSET_TO_INDEX(offset);
    if (blk_i >= self->index_buf_len) {
        struct index_dump_ctx *ctx = kv_malloc(sizeof(struct index_dump_ctx));
        ctx->self = self;
        ctx->index_buf = self->index_buf;
        ctx->offset = index_log->tail;
        kv_circular_log_append(index_log, ctx->index_buf, self->index_buf_len, index_dump_cb, ctx);
        self->index_buf = kv_storage_blk_alloc(index_log->storage, self->index_buf_len);
        kv_memset(self->index_buf, 0xFF, self->index_buf_len * index_log->storage->block_size);
        blk_i -= self->index_buf_len;
    }
    self->index_buf[blk_i * KV_INDEX_LOG_ENTRY_PER_BLOCK + i] = bucket_index;
}
// --- compact ---
#define COMPACT_CON_IO 32
#define COMPACT_WRITE_LEN 256

struct offset_list_entry {
    uint64_t offset;
    struct kv_item *item;
    STAILQ_ENTRY(offset_list_entry) next;
};

struct bucket_entry {
    uint32_t index, offset;
    struct kv_bucket *buckets;
    STAILQ_HEAD(, offset_list_entry) offsets;
    UT_hash_handle hh;
};
struct compact_write_ctx {
    struct kv_value_log *self;
    struct bucket_entry *map;
    uint32_t iocnt;
    uint64_t index_head, index_tail;
};
static void compact_write(bool success, void *arg) {
    struct compact_write_ctx *ctx = arg;
    struct kv_value_log *self = ctx->self;
    assert(success);
    if (--ctx->iocnt == 0) {
        struct bucket_entry *x, *tmp;
        struct kv_bucket_lock_entry *unlock_set = NULL, *lock_entry;
        HASH_ITER(hh, ctx->map, x, tmp) {
            HASH_FIND_INT(self->compact.index_set, &x->index, lock_entry);
            assert(lock_entry);
            HASH_DEL(self->compact.index_set, lock_entry);
            HASH_ADD_INT(unlock_set, index, lock_entry);

            kv_bucket_get_meta(ctx->self->bucket_log, x->index)->bucket_offset = x->offset;
            HASH_DEL(ctx->map, x);
            kv_storage_free(x->buckets);
            kv_free(x);
        }
        kv_bucket_unlock(self->bucket_log, &unlock_set);
        for (; ctx->index_head < ctx->index_tail; ++ctx->index_head) {
            ctx->self->compact.index_buf[ctx->index_head] = UINT32_MAX;
        }
        uint64_t len;
        uint64_t uint_per_blk_shift = ctx->self->blk_shift - KV_VALUE_LOG_UNIT_SHIFT;
        for (len = ctx->self->log.head << uint_per_blk_shift; len < ctx->self->compact.tail; len++)
            if (ctx->self->compact.index_buf[len] != UINT32_MAX) break;
        kv_circular_log_move_head(&ctx->self->log, (len >> uint_per_blk_shift) - ctx->self->log.head);
        kv_free(ctx);
    }
}

static uint64_t index_buf_i_to_offset(struct kv_value_log *self, uint64_t i) {
    uint64_t offset_base = self->index_log.head << (KV_INDEX_LOG_ENTRY_BIT + KV_VALUE_LOG_UNIT_SHIFT);
    return (offset_base + (i << KV_VALUE_LOG_UNIT_SHIFT)) % (self->log.size << self->blk_shift);
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
        if (len <= n) {
            kv_memcpy(dst, src, len);
        } else {
            kv_memcpy(dst, src, n);
            iov[*cnt].iov_len = align(self, len - n);
            iov[(*cnt)++].iov_base = src + n;
        }
    } else {
        iov[*cnt].iov_len = align(self, len);
        iov[(*cnt)++].iov_base = src;
    }
}
// value log compression have 2 restrictions:
// value offset must be dword aligned
// the spacing between 2 value is at least KV_VALUE_LOG_UNIT_SIZE
static void value_compact(struct kv_value_log *self) {
    struct bucket_entry **map = (struct bucket_entry **)&self->compact.bucket_map;
    printf("bucket num:%u\n", HASH_COUNT(*map));
    struct compact_write_ctx *ctx = kv_malloc(sizeof(struct compact_write_ctx));
    ctx->self = self;
    ctx->map = NULL;
    ctx->index_head = self->compact.head;
    ctx->index_tail = self->compact.tail;
    uint64_t iov_len = self->compact.tail - self->compact.head;
    struct iovec *bkt_iov = kv_calloc(iov_len, sizeof(struct iovec)), iov[2];
    struct iovec *val_iov = kv_calloc(iov_len * 2, sizeof(struct iovec));
    uint32_t bkt_iocnt = 0, val_iocnt = 0, bkt_offset = kv_bucket_log_offset(self->bucket_log);
    uint64_t tail = 0;

    for (; self->compact.head < self->compact.tail; ++self->compact.head)
        if (self->compact.index_buf[self->compact.head] != UINT32_MAX) {
            struct bucket_entry *entry;
            HASH_FIND_INT(*map, self->compact.index_buf + self->compact.head, entry);
            if (!entry) continue;
            uint64_t offset = index_buf_i_to_offset(self, self->compact.head);
            struct offset_list_entry *x, *tmp;
            for (x = STAILQ_FIRST(&entry->offsets); x && (tmp = STAILQ_NEXT(x, next), 1); x = tmp) {
                if (x->offset == offset) {
                    uint64_t value_offset = x->item->value_offset >> self->blk_shift;
                    uint64_t value_end = align(self, x->item->value_offset + x->item->value_length);
                    kv_circular_log_fetch(&self->log, value_offset, value_end - value_offset, iov);

                    if (iov[1].iov_len) {
                        uint32_t offset_in_buf = x->item->value_offset & self->blk_mask;
                        uint32_t len = (iov->iov_len << self->blk_shift) - offset_in_buf;
                        append_val_iov(self, val_iov, &val_iocnt, tail, (uint8_t *)iov->iov_base + offset_in_buf, len);
                        append_val_iov(self, val_iov, &val_iocnt, tail, iov[1].iov_base, x->item->value_length - len);
                    } else {
                        uint8_t *src = (uint8_t *)iov->iov_base + (x->item->value_offset & self->blk_mask);
                        append_val_iov(self, val_iov, &val_iocnt, tail, src, x->item->value_length);
                    }
                    x->item->value_offset = (kv_value_log_offset(self) + tail) % (self->log.size << self->blk_shift);
                    index_log_write(self, x->item->value_offset, entry->index);

                    uint64_t next_tail = tail + dword_align(x->item->value_length);
                    if (tail >> KV_VALUE_LOG_UNIT_SHIFT == next_tail >> KV_VALUE_LOG_UNIT_SHIFT)
                        tail = (tail & ~KV_VALUE_LOG_UNIT_MASK) + KV_VALUE_LOG_UNIT_SIZE;
                    else
                        tail = next_tail;

                    STAILQ_REMOVE(&entry->offsets, x, offset_list_entry, next);
                    kv_free(x);
                }
            }
            if (STAILQ_EMPTY(&entry->offsets)) {
                HASH_DEL(*map, entry);
                HASH_ADD_INT(ctx->map, index, entry);
                bkt_iov[bkt_iocnt].iov_base = entry->buckets;
                bkt_iov[bkt_iocnt++].iov_len = entry->buckets->chain_length;
                entry->offset = bkt_offset;
                bkt_offset = (bkt_offset + entry->buckets->chain_length) % self->bucket_log->log.size;
            }
        }

    if (bkt_iocnt) {
        ctx->iocnt = 2;
        kv_bucket_log_writev(self->bucket_log, bkt_iov, bkt_iocnt, compact_write, ctx);
        kv_circular_log_appendv(&self->log, val_iov, val_iocnt, compact_write, ctx);
    } else {
        kv_free(ctx);
    }
    kv_free(bkt_iov);
    kv_free(val_iov);
}

struct read_bucket_ctx {
    struct kv_value_log *self;
    struct bucket_entry *bucket;
};

static void compact_read_bucket_cb(bool success, void *arg) {
    struct read_bucket_ctx *ctx = arg;
    struct kv_value_log *self = ctx->self;
    struct bucket_entry **bucket = (struct bucket_entry **)&self->compact.map_tail;
    struct bucket_entry **map = (struct bucket_entry **)&self->compact.bucket_map;

    assert(success);
    if (ctx->bucket) {
        struct kv_bucket *buckets = ctx->bucket->buckets;
        // verfy_buckets
        struct offset_list_entry *x, *tmp;
        for (x = STAILQ_FIRST(&ctx->bucket->offsets); x && (tmp = STAILQ_NEXT(x, next), 1); x = tmp) {
            for (struct kv_bucket *bucket = buckets; bucket - buckets < buckets->chain_length; ++bucket)
                for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
                    if ((item->value_offset & ~KV_VALUE_LOG_UNIT_MASK) == x->offset) {
                        x->item = item;
                        goto next_offset;
                    }
            // offset not finds
            STAILQ_REMOVE(&ctx->bucket->offsets, x, offset_list_entry, next);
            kv_free(x);
        next_offset:;
        }
        if (STAILQ_EMPTY(&ctx->bucket->offsets)) {
            struct kv_bucket_lock_entry *unlock_set = NULL, *lock_entry;
            HASH_FIND_INT(self->compact.index_set, &ctx->bucket->index, lock_entry);
            assert(lock_entry);
            HASH_DEL(self->compact.index_set, lock_entry);
            HASH_ADD_INT(unlock_set, index, lock_entry);
            kv_bucket_unlock(self->bucket_log, &unlock_set);

            kv_storage_free(buckets);
            HASH_DEL(*map, ctx->bucket);
            kv_storage_free(ctx->bucket->buckets);
            kv_free(ctx->bucket);
        }
        for (; self->compact.tail < self->compact.index_blk_num * KV_INDEX_LOG_ENTRY_PER_BLOCK; ++self->compact.tail)
            if (self->compact.index_buf[self->compact.tail] != UINT32_MAX) {
                struct bucket_entry *entry;
                HASH_FIND_INT(*map, self->compact.index_buf + self->compact.tail, entry);
                if (entry && STAILQ_FIRST(&entry->offsets)->item == NULL) break;
            }
        if (self->compact.tail - self->compact.head > COMPACT_WRITE_LEN) {
            value_compact(self);
        }
    }
    if (*bucket) {
        ctx->bucket = *bucket;
        struct kv_bucket_meta *meta = kv_bucket_get_meta(self->bucket_log, ctx->bucket->index);
        ctx->bucket->buckets = kv_storage_blk_alloc(self->bucket_log->log.storage, meta->chain_length);
        kv_bucket_log_read(self->bucket_log, ctx->bucket->index, ctx->bucket->buckets, compact_read_bucket_cb, arg);
        *bucket = (*bucket)->hh.next;
    } else {
        // sync all tasks
        if (--self->compact.state.io_cnt == 0) {
            value_compact(self);
            assert(HASH_COUNT(*map) == 0);
        }
        kv_free(ctx);
    }
}
static void compact_lock_cb(void *arg) {
    struct kv_value_log *self = arg;
    self->compact.map_tail = self->compact.bucket_map;
    self->compact.tail = 0;
    self->compact.head = 0;
    self->compact.state.io_cnt = COMPACT_CON_IO;
    for (size_t i = 0; i < COMPACT_CON_IO; i++) {
        struct read_bucket_ctx *ctx = kv_malloc(sizeof(struct read_bucket_ctx));  // free!!
        ctx->self = self;
        ctx->bucket = NULL;
        compact_read_bucket_cb(true, ctx);
    }
}

static void compact_read_index_cb(bool success, void *arg) {
    struct kv_value_log *self = arg;
    assert(success);
    assert(self->compact.index_set == NULL);
    struct bucket_entry **map = (struct bucket_entry **)&self->compact.bucket_map;
    uint64_t offset_base = self->index_log.head << (KV_INDEX_LOG_ENTRY_BIT + KV_VALUE_LOG_UNIT_SHIFT);
    for (size_t i = 0; i < self->compact.index_blk_num * KV_INDEX_LOG_ENTRY_PER_BLOCK; i++) {
        if (self->compact.index_buf[i] != UINT32_MAX) {
            struct bucket_entry *entry;
            HASH_FIND_INT(*map, self->compact.index_buf + i, entry);
            if (entry == NULL) {  // likely
                entry = kv_malloc(sizeof(struct bucket_entry));
                entry->index = self->compact.index_buf[i];
                STAILQ_INIT(&entry->offsets);
                HASH_ADD_INT(*map, index, entry);
                kv_bucket_lock_add_index(&self->compact.index_set, entry->index);
            }
            struct offset_list_entry *list_entry = kv_malloc(sizeof(struct offset_list_entry));
            list_entry->offset = (offset_base + (i << KV_VALUE_LOG_UNIT_SHIFT)) % (self->log.size << self->blk_shift);
            list_entry->item = NULL;
            STAILQ_INSERT_TAIL(&entry->offsets, list_entry, next);
        }
    }
    kv_bucket_lock(self->bucket_log, self->compact.index_set, compact_lock_cb, self);
}

// index log prefetch?

/**
    read index log
    covert index_buf to map<index, list<offset>>
    concurrent lock -> read bucket log -> find item -> unlock if value is invalid
    sync (256 buckets)
    compact
    concurrent write value log (update index log at the same time)
    write bucket log
**/
static void compact(struct kv_value_log *self) {
    if (self->compact.state.running || !self->bucket_log ||
        kv_circular_log_empty_space(&self->log) >= self->compact.val_buf_len << 2)
        return;
    self->compact.state.running = true;
    assert(self->compact.state.io_cnt == 0);
    kv_circular_log_read(&self->index_log, self->index_log.head, self->compact.index_buf, self->compact.index_blk_num,
                         compact_read_index_cb, self);
}

//--- write ---
void kv_value_log_write(struct kv_value_log *self, int32_t bucket_index, uint8_t *value, uint32_t value_length,
                        kv_circular_log_io_cb cb, void *cb_arg) {
    index_log_write(self, kv_value_log_offset(self), bucket_index);
    kv_circular_log_append(&self->log, value, align(self, value_length), cb, cb_arg);
    // compact(self);
}