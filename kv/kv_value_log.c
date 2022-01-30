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
#define COMPACT_CON_IO 4U
#define COMPACT_CON_READ 512U
#define COMPACT_WRITE_LEN 256U

struct compact_ctx {
    struct kv_value_log *self;
    struct kv_bucket_lock_entry *index_set;
    uint32_t iocnt;
    struct bucket_list_head bucket_list;
};
#define TAILQ_FOREACH_SAFE(var, head, field, tvar) \
    for ((var) = TAILQ_FIRST((head)); (var) && ((tvar) = TAILQ_NEXT((var), field), 1); (var) = (tvar))
static void prefetch_bucket(void *arg);
static void get_bucket_cb(struct kv_bucket_pool *pool, void *arg) {
    struct bucket_list_entry *list_entry = arg;
    list_entry->entry = pool;
    list_entry->self->bucket_prefetch_io_cnt--;
    list_entry->self->valid_bucket_list_size++;
    prefetch_bucket(list_entry->self);
}
static void prefetch_bucket(void *arg) {
    struct kv_value_log *self = arg;
    while (true) {
        uint32_t index_log_offset = self->prefetch_tail >> KV_INDEX_LOG_ENTRY_BIT;
        if (!kv_circular_log_is_fetchable(&self->index_log, index_log_offset)) return;
        uint32_t *index_buf;
        kv_circular_log_fetch_one(&self->index_log, index_log_offset, (void **)&index_buf);
        for (size_t i = self->prefetch_tail & KV_INDEX_LOG_ENTRY_MASK; i < KV_INDEX_LOG_ENTRY_PER_BLOCK; i++) {
            if (self->bucket_prefetch_io_cnt >= COMPACT_CON_READ) return;
            if (self->bucket_list_size >= 4 * COMPACT_CON_IO * COMPACT_WRITE_LEN) return;
            if (index_buf[i] == UINT32_MAX) {
                self->prefetch_tail = (self->prefetch_tail + 1) % (self->index_log.size << KV_INDEX_LOG_ENTRY_BIT);
                continue;
            }
            struct bucket_list_entry *list_entry = kv_malloc(sizeof(struct bucket_list_entry));
            TAILQ_INSERT_TAIL(&self->bucket_list, list_entry, next);
            list_entry->self = self;
            list_entry->value_offset = self->prefetch_tail << KV_VALUE_LOG_UNIT_SHIFT;
            self->bucket_list_size++;
            self->bucket_prefetch_io_cnt++;
            self->prefetch_tail = (self->prefetch_tail + 1) % (self->index_log.size << KV_INDEX_LOG_ENTRY_BIT);
            kv_bucket_pool_get(self->bucket_log, index_buf[i], get_bucket_cb, list_entry);
        }
    }
}

static inline void del_list_entry(struct kv_value_log *self, struct bucket_list_head *list_head,
                                  struct bucket_list_entry *list_entry) {
    uint32_t *index_buf;
    uint32_t tail = list_entry->value_offset >> KV_VALUE_LOG_UNIT_SHIFT;
    kv_circular_log_fetch_one(&self->index_log, tail >> KV_INDEX_LOG_ENTRY_BIT, (void **)&index_buf);
    index_buf[tail & KV_INDEX_LOG_ENTRY_MASK] = UINT32_MAX;
    TAILQ_REMOVE(list_head, list_entry, next);
    kv_free(list_entry);
}

static void compact_write(bool success, void *arg) {
    struct compact_ctx *ctx = arg;
    struct kv_value_log *self = ctx->self;
    assert(success);
    if (--ctx->iocnt) return;
    kv_bucket_unlock(self->bucket_log, &ctx->index_set);

    struct bucket_list_entry *list_entry, *tmp;
    TAILQ_FOREACH_SAFE(list_entry, &ctx->bucket_list, next, tmp) { del_list_entry(self, &ctx->bucket_list, list_entry); }

    uint64_t n = 0;
    for (n = 0; true; n += KV_VALUE_LOG_UNIT_SIZE) {
        uint32_t *index_buf;
        uint32_t tail =
            ((self->log.head << self->blk_shift) + n) % (self->log.size << self->blk_shift) >> KV_VALUE_LOG_UNIT_SHIFT;
        kv_circular_log_fetch_one(&self->index_log, tail >> KV_INDEX_LOG_ENTRY_BIT, (void **)&index_buf);
        if (index_buf[tail & KV_INDEX_LOG_ENTRY_MASK] != UINT32_MAX) break;
    }
    kv_circular_log_move_head(&self->log, n >> self->blk_shift);
    kv_circular_log_move_head(&self->index_log, n >> (KV_INDEX_LOG_ENTRY_BIT + KV_VALUE_LOG_UNIT_SHIFT));
    self->compact_io_cnt--;
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

    // value verfiy
    uint32_t bucket_list_size = 0;
    struct kv_bucket_lock_entry *unlock_set = NULL;
    struct bucket_list_entry *list_entry, *tmp;
    TAILQ_FOREACH_SAFE(list_entry, &ctx->bucket_list, next, tmp) {
        bucket_list_size++;
        struct kv_bucket_chain_entry *ce;
        TAILQ_FOREACH(ce, &list_entry->entry->buckets, entry) {
            for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket)
                for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item) {
                    if (KV_EMPTY_ITEM(item)) continue;
                    if ((item->value_offset & ~KV_VALUE_LOG_UNIT_MASK) == list_entry->value_offset) {
                        list_entry->item = item;
                        goto valid_value;
                    }
                }
        }
        // invalid
        bucket_list_size--;
        struct kv_bucket_lock_entry *lock_entry;
        HASH_FIND_INT(ctx->index_set, &TAILQ_FIRST(&list_entry->entry->buckets)->bucket->index, lock_entry);
        assert(lock_entry);
        if (--lock_entry->ref_cnt == 0) {
            HASH_DEL(ctx->index_set, lock_entry);
            HASH_ADD_INT(unlock_set, index, lock_entry);
        }
        kv_bucket_pool_put(self->bucket_log, list_entry->entry, false, NULL, NULL);
        del_list_entry(self, &ctx->bucket_list, list_entry);
    valid_value:;
    }
    if (unlock_set) kv_bucket_unlock(self->bucket_log, &unlock_set);

    if (bucket_list_size == 0) {
        self->compact_io_cnt--;
        kv_free(ctx);
        return;
    }
    // value compression
    struct iovec iov[2], *val_iov = kv_calloc(bucket_list_size + 2, sizeof(struct iovec));
    uint64_t tail = 0;
    uint32_t iovcnt = 0;
    struct kv_bucket_pool_head pool_head;
    STAILQ_INIT(&pool_head);
    TAILQ_FOREACH(list_entry, &ctx->bucket_list, next) {
        struct kv_item *item = list_entry->item;
        uint64_t value_offset = item->value_offset >> self->blk_shift;
        uint64_t value_end = align(self, item->value_offset + item->value_length);
        if (!kv_circular_log_is_fetchable(&self->log, value_end)) {
            fprintf(stderr, "value_log_compression: value log prefetch is too slow.\n");
            exit(-1);
        }
        kv_circular_log_fetch(&self->log, value_offset, value_end - value_offset, iov);

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
        index_log_write(self, item->value_offset, TAILQ_FIRST(&list_entry->entry->buckets)->bucket->index);

        uint64_t next_tail = tail + dword_align(item->value_length);
        if (tail >> KV_VALUE_LOG_UNIT_SHIFT == next_tail >> KV_VALUE_LOG_UNIT_SHIFT)
            tail = (tail & ~KV_VALUE_LOG_UNIT_MASK) + KV_VALUE_LOG_UNIT_SIZE;
        else
            tail = next_tail;

        struct kv_bucket_lock_entry *lock_entry;
        HASH_FIND_INT(ctx->index_set, &TAILQ_FIRST(&list_entry->entry->buckets)->bucket->index, lock_entry);
        assert(lock_entry);
        if (--lock_entry->ref_cnt == 0) {
            STAILQ_INSERT_TAIL(&pool_head, list_entry->entry, next);
        }
    }
    ctx->iocnt = 2;
    kv_bucket_pool_put_bulk(self->bucket_log, &pool_head, compact_write, ctx);
    kv_circular_log_appendv(&self->log, val_iov, iovcnt, compact_write, ctx);
    kv_free(val_iov);
}

static void compact(struct kv_value_log *self) {
    prefetch_bucket(self);
    if (!self->bucket_log || kv_circular_log_empty_space(&self->log) >= COMPACT_WRITE_LEN * COMPACT_CON_IO * 8) return;
    if (self->compact_io_cnt >= COMPACT_CON_IO) return;
    if (self->valid_bucket_list_size < COMPACT_WRITE_LEN) {
        fprintf(stderr, "value_log_compression: bucket prefetch is too slow.\n");
        exit(-1);
    }
    self->compact_io_cnt++;
    struct compact_ctx *ctx = kv_malloc(sizeof(struct compact_ctx));
    ctx->self = self;
    ctx->index_set = NULL;
    TAILQ_INIT(&ctx->bucket_list);
    for (size_t i = 0; i < COMPACT_WRITE_LEN; i++) {
        struct bucket_list_entry *list_entry = TAILQ_FIRST(&self->bucket_list);
        TAILQ_REMOVE(&self->bucket_list, list_entry, next);
        kv_bucket_lock_add_index(&ctx->index_set, TAILQ_FIRST(&list_entry->entry->buckets)->bucket->index);
        TAILQ_INSERT_TAIL(&ctx->bucket_list, list_entry, next);
    }
    self->valid_bucket_list_size -= COMPACT_WRITE_LEN;
    self->bucket_list_size -= COMPACT_WRITE_LEN;
    kv_bucket_lock(self->bucket_log, ctx->index_set, compact_lock_cb, ctx);
}

//--- write ---
void kv_value_log_write(struct kv_value_log *self, int32_t bucket_index, uint8_t *value, uint32_t value_length,
                        kv_circular_log_io_cb cb, void *cb_arg) {
    index_log_write(self, kv_value_log_offset(self), bucket_index);
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

    uint64_t index_log_block_num = size / (KV_VALUE_LOG_UNIT_SIZE + sizeof(uint32_t)) / KV_INDEX_LOG_ENTRY_PER_BLOCK;
    kv_circular_log_init(&self->index_log, storage, base >> self->blk_shift, index_log_block_num, 0, 0, index_buf_len * 2,
                         index_buf_len / 8);
    uint64_t units_per_block = storage->block_size / KV_VALUE_LOG_UNIT_SIZE;
    uint64_t block_num = index_log_block_num * KV_INDEX_LOG_ENTRY_PER_BLOCK / units_per_block;

    self->bucket_log = bucket_log;
    self->index_buf_len = index_buf_len;
    self->index_buf = kv_storage_blk_alloc(storage, self->index_buf_len);
    kv_memset(self->index_buf, 0xFF, self->index_buf_len * storage->block_size);

    kv_circular_log_init(&self->log, storage, self->index_log.base + self->index_log.size, block_num, 0, 0,
                         COMPACT_WRITE_LEN * COMPACT_CON_IO * 16, 512);

    TAILQ_INIT(&self->bucket_list);
}

void kv_value_log_fini(struct kv_value_log *self) {
    kv_storage_free(self->index_buf);
    kv_circular_log_fini(&self->log);
}
