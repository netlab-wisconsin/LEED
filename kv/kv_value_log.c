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
                       uint64_t size, uint32_t append_buf_len) {
    kv_memset(self, 0, sizeof(struct kv_value_log));
    for (self->blk_shift = 0; !((storage->block_size >> self->blk_shift) & 1); ++self->blk_shift)
        ;
    assert(storage->block_size == 1U << self->blk_shift);
    self->blk_mask = storage->block_size - 1;
    assert(!(base & self->blk_mask || size & self->blk_mask));

    uint64_t index_log_block_num = size / (KV_VALUE_LOG_UNIT_SIZE + sizeof(uint32_t)) / KV_INDEX_LOG_ENTRY_PER_BLOCK;
    kv_circular_log_init(&self->index_log, storage, base >> self->blk_shift, index_log_block_num, 0, 0);
    uint64_t units_per_block = storage->block_size / KV_VALUE_LOG_UNIT_SIZE;
    uint64_t block_num = index_log_block_num * KV_INDEX_LOG_ENTRY_PER_BLOCK / units_per_block;
    kv_circular_log_init(&self->log, storage, self->index_log.base + self->index_log.size, block_num, 0, 0);
    for (size_t i = 0; i < 2; i++) {
        self->append_buf[i] = kv_storage_blk_alloc(self->log.storage, append_buf_len);
        for (size_t j = 0; j < append_buf_len * KV_INDEX_LOG_ENTRY_PER_BLOCK; j++) self->append_buf[i][j] = UINT32_MAX;
    }
    self->bucket_log = bucket_log;
    self->buf_len = append_buf_len;
    self->compact.index_blk_num = 256;  // TODO: change this
    self->compact.index_buf = kv_storage_blk_alloc(storage, self->compact.index_blk_num);
    self->compact.val_buf_len = self->compact.index_blk_num * KV_INDEX_LOG_ENTRY_PER_BLOCK / units_per_block;
    self->compact.val_buf = kv_storage_blk_alloc(storage, self->compact.val_buf_len);
}

void kv_value_log_fini(struct kv_value_log *self) { kv_circular_log_fini(&self->log); }

// --- index log write ---
#define OFFSET_TO_BLOCK(offset) ((offset) >> (KV_INDEX_LOG_ENTRY_BIT + KV_VALUE_LOG_UNIT_SHIFT))
#define OFFSET_TO_INDEX(offset) (((offset) >> KV_VALUE_LOG_UNIT_SHIFT) & KV_INDEX_LOG_ENTRY_MASK)

#define BUF(self) ((self)->append_buf[(self)->append_buf_i])
#define PREFETCH_BUF(self) ((self)->append_buf[!(self)->append_buf_i])

static void dump_cb(bool success, void *arg) {
    struct kv_value_log *self = arg;
    struct kv_circular_log *index_log = &self->index_log;
    if (success) {
        self->index_log_dump_running = false;
        index_log->tail = (index_log->tail + self->buf_len) % index_log->size;
        kv_memset(PREFETCH_BUF(self), 0xFF, self->buf_len * KV_INDEX_LOG_ENTRY_PER_BLOCK * sizeof(uint32_t));
    } else {
        fprintf(stderr, "kv_index_log dump fail!");
        kv_circular_log_write(index_log, index_log->tail, PREFETCH_BUF(self), self->buf_len, dump_cb, self);
    }
}

static void index_log_write(struct kv_value_log *self, uint64_t offset, uint32_t bucket_index) {
    struct kv_circular_log *index_log = &self->index_log;
    uint64_t blk_i = (index_log->size - index_log->tail + OFFSET_TO_BLOCK(offset)) % index_log->size;
    if (self->index_log_dump_running) blk_i--;
    assert(blk_i < 2 * self->buf_len);
    uint64_t i = OFFSET_TO_INDEX(offset);
    if (blk_i < self->buf_len) {
        BUF(self)[blk_i * KV_INDEX_LOG_ENTRY_PER_BLOCK + i] = bucket_index;
    } else {
        assert(!self->index_log_dump_running);  // TODO: may wait for dump task finish.
        self->index_log_dump_running = true;
        self->append_buf_i = !self->append_buf_i;
        kv_circular_log_write(index_log, index_log->tail, PREFETCH_BUF(self), self->buf_len, dump_cb, self);
        BUF(self)[(blk_i - 1) * KV_INDEX_LOG_ENTRY_PER_BLOCK + i] = bucket_index;
    }
}
// --- compact ---
#define COMPACT_CON_IO 32
#define COMPACT_READ_LEN 256

struct offset_list_entry {
    uint64_t offset;
    struct kv_item *item;
    STAILQ_ENTRY(offset_list_entry) next;
};

struct bucket_entry {
    uint32_t index;
    struct kv_bucket *buckets;
    STAILQ_HEAD(, offset_list_entry) offsets;
    UT_hash_handle hh;
};
static void value_compact(struct kv_value_log *self) {
    struct bucket_entry *map = self->compact.bucket_map;
    printf("bucket num:%u\n", HASH_COUNT(map));
}

struct read_bucket_ctx {
    struct kv_value_log *self;
    struct bucket_entry *bucket;
};

static void compact_read_bucket_cb(bool success, void *arg);

static void compact_lock_cb(void *arg) {
    struct kv_value_log *self = ((struct read_bucket_ctx *)arg)->self;
    struct bucket_entry *bucket = ((struct read_bucket_ctx *)arg)->bucket;
    kv_bucket_log_read(self->bucket_log, bucket->index, bucket->buckets, compact_read_bucket_cb, arg);
}

static void compact_read_bucket_cb(bool success, void *arg) {
    struct read_bucket_ctx *ctx = arg;
    struct kv_value_log *self = ctx->self;
    struct bucket_entry **bucket = (struct bucket_entry **)&self->compact.current_bucket;
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
            kv_bucket_unlock(self->bucket_log, ctx->bucket->index);
            kv_storage_free(buckets);
            HASH_DEL(*map, ctx->bucket);
            kv_free(ctx->bucket);
        }
    }
    if (*bucket) {
        ctx->bucket = *bucket;
        struct kv_bucket_meta *meta = kv_bucket_get_meta(self->bucket_log, ctx->bucket->index);
        ctx->bucket->buckets = kv_storage_blk_alloc(self->bucket_log->log.storage, meta->chain_length);
        kv_bucket_lock(self->bucket_log, ctx->bucket->index, compact_lock_cb, arg);
        *bucket = (*bucket)->hh.next;
    } else {
        // sync all tasks
        if (--self->compact.state.io_cnt == 0) value_compact(self);
    }
}

static void compact_read_index_cb(bool success, void *arg) {
    struct kv_value_log *self = arg;
    assert(success);
    --self->compact.state.io_cnt;
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
            }
            struct offset_list_entry *list_entry = kv_malloc(sizeof(struct offset_list_entry));
            list_entry->offset = (offset_base + (i << KV_VALUE_LOG_UNIT_SHIFT)) % (self->log.size << self->blk_shift);
            STAILQ_INSERT_TAIL(&entry->offsets, list_entry, next);
        }
    }
    self->compact.current_bucket = self->compact.bucket_map;
    self->compact.state.io_cnt += COMPACT_CON_IO;
    for (size_t i = 0; i < COMPACT_CON_IO; i++) {
        struct read_bucket_ctx *ctx = kv_malloc(sizeof(struct read_bucket_ctx));
        ctx->self = self;
        ctx->bucket = NULL;
        compact_read_bucket_cb(true, ctx);
    }
}

static void compact_read_value_cb(bool success, void *arg) {
    struct kv_value_log *self = arg;
    assert(success);
    uint64_t index = (self->compact.val_buf_head - self->compact.val_buf) >> self->blk_shift;
    if (index == self->compact.val_buf_len) {
        // sync all tasks
        if (--self->compact.state.io_cnt == 0) value_compact(self);
    } else {
        uint64_t offset = (self->log.head + index) % self->log.size;
        uint64_t remaining_len = self->compact.val_buf_len - index;
        uint64_t n = remaining_len < COMPACT_READ_LEN ? remaining_len : COMPACT_READ_LEN;
        kv_circular_log_read(&self->log, offset, self->compact.val_buf_head, n, compact_read_value_cb, self);
        self->compact.val_buf_head += n << self->blk_shift;
    }
}
// value log prefetch?
// index log prefetch?

/**
    read index log && concurrent read value log
    covert index_buf to map<index, list<offset>>
    concurrent lock -> read bucket log -> find item -> unlock if value is invalid
    sync all tasks ??
    compact
    concurrent write value log (update index log at the same time)
    write bucket log
**/
static void compact(struct kv_value_log *self) {
    if (self->compact.state.running || !self->bucket_log ||
        kv_circular_log_empty_space(&self->log) << self->blk_shift >= self->compact.val_buf_len << 7)
        return;
    self->compact.state.running = true;
    assert(self->compact.state.io_cnt == 0);
    self->compact.state.io_cnt++;
    kv_circular_log_read(&self->index_log, self->index_log.head, self->compact.index_buf, self->compact.index_blk_num,
                         compact_read_index_cb, self);
    self->compact.state.io_cnt += COMPACT_CON_IO;
    self->compact.val_buf_head = self->compact.val_buf;
    for (size_t i = 0; i < COMPACT_CON_IO; i++) compact_read_value_cb(true, self);
}

//--- write ---
void kv_value_log_write(struct kv_value_log *self, int32_t bucket_index, uint8_t *value, uint32_t value_length,
                        kv_circular_log_io_cb cb, void *cb_arg) {
    index_log_write(self, kv_value_log_offset(self), bucket_index);
    kv_circular_log_append(&self->log, value, align(self, value_length), cb, cb_arg);
    compact(self);
}