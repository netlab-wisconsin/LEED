#include "kv_data_store.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>

#include "kv_app.h"
#include "kv_waiting_map.h"
#include "memery_operation.h"

#define INIT_CON_IO_NUM 128

// --- init & fini ---
static void init(struct kv_data_store *self, kv_data_store_cb cb, void *cb_arg) {
    self->compact_buffer = kv_storage_blk_alloc(self->bucket_log.log.storage, COMPACT_BUCKET_NUM);
    self->bucket_meta = kv_calloc(self->bucket_num, sizeof(struct kv_bucket_meta));
    kv_memset(self->bucket_meta, 0, sizeof(struct kv_bucket_meta) * self->bucket_num);
    for (size_t i = 0; i < self->bucket_num; i++) {
        self->bucket_meta[i].bucket_offset = self->bucket_log.log.head + i;
        self->bucket_meta[i].chain_length = 1;
    }
    self->waiting_map = NULL;
    if (cb) cb(true, cb_arg);
}
struct init_ctx {
    struct kv_data_store *self;
    kv_data_store_cb cb;
    void *cb_arg;
    uint32_t io_num;
};

struct init_write_ctx {
    struct kv_bucket buckets[COMPACT_BUCKET_NUM];
    struct init_ctx *init_ctx;
};

static void init_write_cb(bool success, void *arg) {
    struct init_write_ctx *ctx = arg;
    struct kv_data_store *self = ctx->init_ctx->self;
    assert(success);  // TODO: error handling
    uint32_t n = self->bucket_log.log.size - self->bucket_log.tail;
    if (n == 0) {
        if (--ctx->init_ctx->io_num == 0) {
            init(self, ctx->init_ctx->cb, ctx->init_ctx->cb_arg);
            kv_free(ctx->init_ctx);
        }
        kv_storage_free(ctx);
        return;
    }
    if (n <= COMPACT_BUCKET_NUM)
        kv_bucket_log_move_head(&self->bucket_log, self->extra_bucket_num);
    else
        n = COMPACT_BUCKET_NUM;
    for (size_t i = 0; i < n; i++) {
        if (self->bucket_log.tail + i < self->extra_bucket_num)
            ctx->buckets[i].index = UINT32_MAX;
        else
            ctx->buckets[i].index = self->bucket_log.tail + i - self->extra_bucket_num;
        ctx->buckets[i].chain_length = 1;
    }
    kv_bucket_log_write(&self->bucket_log, ctx->buckets, n, init_write_cb, ctx);
}

void kv_data_store_init(struct kv_data_store *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets,
                        uint64_t value_log_block_num, kv_data_store_cb cb, void *cb_arg) {
    // assert(base + size <= storage->num_blocks);
    size_t log_num_buckets = 0;
    while (((size_t)1 << log_num_buckets) < num_buckets) log_num_buckets++;
    assert(log_num_buckets <= 32);
    self->bucket_num = (size_t)1 << log_num_buckets;

    self->buckets_mask = self->bucket_num - 1;
    self->extra_bucket_num = self->bucket_num;
    num_buckets = self->extra_bucket_num + self->bucket_num;
    if (num_buckets + value_log_block_num > storage->num_blocks) {
        fprintf(stderr, "kv_data_store_init: Not enough space.\n");
        if (cb) cb(false, cb_arg);
        return;
    }

    kv_bucket_log_init(&self->bucket_log, storage, base, num_buckets, 0, 0);
    kv_value_log_init(&self->value_log, storage, (base + num_buckets) * storage->block_size,
                      value_log_block_num * storage->block_size, 0, 0);
    printf("bucket log size: %lf GB\n", ((double)num_buckets) * storage->block_size / (1 << 30));
    printf("value log size: %lf GB\n", ((double)value_log_block_num) * storage->block_size / (1 << 30));
    struct init_ctx *ctx = kv_malloc(sizeof(struct init_ctx));
    ctx->self = self;
    ctx->cb = cb;
    ctx->cb_arg = cb_arg;
    ctx->io_num = INIT_CON_IO_NUM;
    for (size_t i = 0; i < INIT_CON_IO_NUM; i++) {
        struct init_write_ctx *buf = kv_storage_zmalloc(storage, sizeof(struct init_write_ctx));
        buf->init_ctx = ctx;
        init_write_cb(true, buf);
    }
}

void kv_data_store_fini(struct kv_data_store *self) {
    kv_free(self->bucket_meta);
    kv_storage_free(self->compact_buffer);
    kv_bucket_log_fini(&self->bucket_log);
    kv_value_log_fini(&self->value_log);
}

// --- lock & unlock ---
static void bucket_lock(struct kv_data_store *self, uint32_t index, kv_task_cb cb, void *cb_arg) {
    assert(index < self->bucket_num);
    assert(cb);
    if (self->bucket_meta[index].lock) {
        kv_waiting_map_put(&self->waiting_map, index, cb, cb_arg);
    } else {
        self->bucket_meta[index].lock = 1;
        cb(cb_arg);
    }
}
static bool bucket_lock_nowait(struct kv_data_store *self, uint32_t index) {
    assert(index < self->bucket_num);
    if (self->bucket_meta[index].lock) return false;
    self->bucket_meta[index].lock = 1;
    return true;
}
static void bucket_unlock(struct kv_data_store *self, uint32_t index) {
    assert(index < self->bucket_num);
    assert(self->bucket_meta[index].lock);
    struct kv_waiting_task task = kv_waiting_map_get(&self->waiting_map, index);
    if (task.cb)
        kv_app_send_msg(kv_app_get_thread(), task.cb, task.cb_arg);
    else
        self->bucket_meta[index].lock = 0;
}
#define kv_data_store_bucket_index(self, key) (*(uint32_t *)(key)&self->buckets_mask)
static inline bool compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len) {
    return key1_len == key2_len && !kv_memcmp8(key1, key2, key1_len);
}
// --- debug ---
static inline void verify_buckets(struct kv_bucket *buckets, uint32_t index, uint8_t length) {
    assert(length);
    for (size_t i = 0; i < length; i++) {
        assert(buckets[i].chain_index == i);
        assert(buckets[i].chain_length == length);
        assert(buckets[i].index == index);
    }
}
// --- compact ---
static void compact_writev_cb(bool success, void *arg) {
    struct kv_data_store *self = arg;
    if (!success) {
        fprintf(stderr, "compact_writev_cb: IO error!\n");
        self->is_compact_task_running = false;
        return;
    }
    uint32_t n = 0;
    for (size_t i = 0; i < self->compact_iovcnt; i++) {
        struct kv_bucket *bucket = self->compact_iov[i].iov_base;
        self->bucket_meta[bucket->index].bucket_offset = (self->compact_offset + n) % self->bucket_log.log.size;
        bucket_unlock(self, bucket->index);
        verify_buckets(bucket, bucket->index, self->bucket_meta[bucket->index].chain_length);
        n += self->compact_iov[i].iov_len;
    }
    kv_bucket_log_move_head(&self->bucket_log, self->compact_length);
    //compression ratio: n/self->compact_length
    self->is_compact_task_running = false;
}
static void compact_read_cb(bool success, void *arg) {
    struct kv_data_store *self = arg;
    if (!success) {
        fprintf(stderr, "compact_read_cb: IO error!\n");
        self->is_compact_task_running = false;
        return;
    }
    self->compact_iovcnt = 0;
    self->compact_length = COMPACT_BUCKET_NUM;
    for (size_t i = 0; i < COMPACT_BUCKET_NUM; i += self->compact_buffer[i].chain_length) {
        assert(self->compact_buffer[i].chain_index == 0);
        uint32_t bucket_offset = (self->bucket_log.log.head + i) % self->bucket_log.log.size;
        struct kv_bucket_meta *meta = self->bucket_meta + self->compact_buffer[i].index;
        if (self->compact_buffer[i].chain_length + i > COMPACT_BUCKET_NUM) {
            if (meta->bucket_offset == bucket_offset && !meta->lock)
                self->compact_length = i;
            else
                self->compact_length = self->compact_buffer[i].chain_length + i;
            break;
        }
        if (meta->bucket_offset == bucket_offset && bucket_lock_nowait(self, self->compact_buffer[i].index))
            self->compact_iov[self->compact_iovcnt++] =
                (struct iovec){self->compact_buffer + i, self->compact_buffer[i].chain_length};
    }
    self->compact_offset = kv_bucket_log_offset(&self->bucket_log);
    if (self->compact_iovcnt)
        kv_bucket_log_writev(&self->bucket_log, self->compact_iov, self->compact_iovcnt, compact_writev_cb, self);
    else
        compact_writev_cb(true, self);
}

static void _compact(void *arg) {
    struct kv_data_store *self = arg;
    // TODO: using bit map
    assert(kv_circular_log_length(&self->bucket_log.log) >= COMPACT_BUCKET_NUM);
    kv_bucket_log_read(&self->bucket_log, self->bucket_log.log.head, self->compact_buffer, COMPACT_BUCKET_NUM, compact_read_cb,
                       self);
}
static inline void compact(struct kv_data_store *self) {
    if (kv_circular_log_length(&self->bucket_log.log) >= 7 * self->bucket_log.log.size / 8 && !self->is_compact_task_running) {
        self->is_compact_task_running = true;
        kv_app_send_msg(kv_app_get_thread(), _compact, self);
    }
}

//--- find item ---

typedef void (*_find_item_cb)(bool success, struct kv_item *located_item, struct kv_bucket *located_bucket, void *cb_arg);
struct find_item_ctx {
    struct kv_data_store *self;
    uint32_t bucket_index;
    uint8_t *key;
    uint8_t key_length;
    struct kv_bucket *buckets;
    _find_item_cb cb;
    void *cb_arg;
};
#define find_item_ctx_init(ctx, self, bucket_index, key, key_length, buckets, cb, cb_arg) \
    do {                                                                                  \
        ctx->self = self;                                                                 \
        ctx->bucket_index = bucket_index;                                                 \
        ctx->key = key;                                                                   \
        ctx->key_length = key_length;                                                     \
        ctx->buckets = buckets;                                                           \
        ctx->cb = cb;                                                                     \
        ctx->cb_arg = cb_arg;                                                             \
    } while (0)

static void find_item_read_cb(bool success, void *arg) {
    struct find_item_ctx *ctx = arg;
    if (!success) {
        ctx->cb(false, NULL, NULL, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    verify_buckets(ctx->buckets, ctx->bucket_index, ctx->self->bucket_meta[ctx->bucket_index].chain_length);
    for (struct kv_bucket *bucket = ctx->buckets; bucket - ctx->buckets < ctx->buckets->chain_length; ++bucket) {
        for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item) {
            // if(KV_EMPTY_ITEM(item)) continue; // empty item key_length != ctx->key_length
            if (compare_keys(item->key, item->key_length, ctx->key, ctx->key_length)) {
                // job done!
                ctx->cb(true, item, bucket, ctx->cb_arg);
                kv_free(ctx);
                return;
            }
        }
    }
    // item not found
    ctx->cb(true, NULL, NULL, ctx->cb_arg);
    kv_free(ctx);
}
static void find_item(struct kv_data_store *self, uint32_t bucket_index, uint8_t *key, uint8_t key_length,
                      struct kv_bucket *buckets, _find_item_cb cb, void *cb_arg) {
    assert(cb);
    struct kv_bucket_meta *meta = self->bucket_meta + bucket_index;
    assert(meta->chain_length);
    struct find_item_ctx *ctx = kv_malloc(sizeof(struct find_item_ctx));
    find_item_ctx_init(ctx, self, bucket_index, key, key_length, buckets, cb, cb_arg);
    kv_bucket_log_read(&self->bucket_log, meta->bucket_offset, buckets, meta->chain_length, find_item_read_cb, ctx);
}
// --- find empty ---
static bool alloc_extra_bucket(struct kv_data_store *self, struct kv_bucket *buckets) {
    uint8_t length = buckets->chain_length;
    if (length == 0x7F) return false;
    if (self->allocated_bucket_num == self->extra_bucket_num) return false;  // almost impossible
    self->allocated_bucket_num++;
    buckets[length].index = buckets->index;
    buckets[length].chain_index = length;
    length++;
    for (size_t i = 0; i < length; i++) buckets[i].chain_length = length;
    return true;
}
static struct kv_item *find_empty(struct kv_data_store *self, struct kv_bucket *buckets) {
    for (struct kv_bucket *bucket = buckets; bucket - buckets < buckets->chain_length; ++bucket)
        for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
            if (KV_EMPTY_ITEM(item)) return item;
    if (alloc_extra_bucket(self, buckets)) return buckets[buckets->chain_length - 1].items;
    return NULL;
}
//--- set ---
struct set_ctx {
    struct kv_data_store *self;
    uint8_t *key;
    uint8_t key_length;
    uint8_t *value;
    uint32_t value_length;
    kv_data_store_cb cb;
    void *cb_arg;
    uint32_t bucket_index;
    struct kv_bucket *buckets;
    uint32_t tail;
};
#define set_ctx_init(ctx, self, key, key_length, value, value_length, cb, cb_arg) \
    do {                                                                          \
        ctx->self = self;                                                         \
        ctx->key = key;                                                           \
        ctx->key_length = key_length;                                             \
        ctx->value = value;                                                       \
        ctx->value_length = value_length;                                         \
        ctx->cb = cb;                                                             \
        ctx->cb_arg = cb_arg;                                                     \
    } while (0)

static void set_bucket_log_writev_cb(bool success, void *arg) {
    struct set_ctx *ctx = arg;
    if (success) {
        struct kv_bucket_meta *meta = ctx->self->bucket_meta + ctx->bucket_index;
        meta->chain_length = ctx->buckets->chain_length;
        meta->bucket_offset = ctx->tail;
        verify_buckets(ctx->buckets, ctx->bucket_index, meta->chain_length);
    }
    bucket_unlock(ctx->self, ctx->bucket_index);
    kv_storage_free(ctx->buckets);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    compact(ctx->self);
    kv_free(ctx);
}

static void set_write_value_log_cb(bool success, void *arg) {
    struct set_ctx *ctx = arg;
    if (!success) {
        fprintf(stderr, "set_write_value_log_cb: IO error.\n");
        bucket_unlock(ctx->self, ctx->bucket_index);
        kv_storage_free(ctx->buckets);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    ctx->tail = kv_bucket_log_offset(&ctx->self->bucket_log);
    kv_bucket_log_write(&ctx->self->bucket_log, ctx->buckets, ctx->buckets->chain_length, set_bucket_log_writev_cb, ctx);
}

static void set_find_item_cb(bool success, struct kv_item *located_item, struct kv_bucket *located_bucket, void *cb_arg) {
    struct set_ctx *ctx = cb_arg;
    if (!success) {
        fprintf(stderr, "set_find_item_cb: IO error.\n");
        bucket_unlock(ctx->self, ctx->bucket_index);
        kv_storage_free(ctx->buckets);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    if (located_item) {  // update
        located_item->value_length = ctx->value_length;
        located_item->value_offset =
            kv_value_log_write(&ctx->self->value_log, ctx->value, ctx->value_length, set_write_value_log_cb, ctx);
    } else {  // new
        if ((located_item = find_empty(ctx->self, ctx->buckets))) {
            located_item->key_length = ctx->key_length;
            kv_memcpy(located_item->key, ctx->key, ctx->key_length);
            located_item->value_length = ctx->value_length;
            located_item->value_offset =
                kv_value_log_write(&ctx->self->value_log, ctx->value, ctx->value_length, set_write_value_log_cb, ctx);
        } else {
            fprintf(stderr, "set_find_item_cb: No more bucket available.\n");
            bucket_unlock(ctx->self, ctx->bucket_index);
            kv_storage_free(ctx->buckets);
            if (ctx->cb) ctx->cb(false, ctx->cb_arg);
            kv_free(ctx);
        }
    }
}

static void set_lock_cb(void *arg) {
    struct set_ctx *ctx = arg;
    struct kv_storage *storage = ctx->self->bucket_log.log.storage;
    // in case a new bucket is allocated.
    ctx->buckets = kv_storage_zblk_alloc(storage, ctx->self->bucket_meta[ctx->bucket_index].chain_length + 1);
    find_item(ctx->self, ctx->bucket_index, ctx->key, ctx->key_length, ctx->buckets, set_find_item_cb, ctx);
}
void kv_data_store_set(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t value_length,
                       kv_data_store_cb cb, void *cb_arg) {
    struct set_ctx *ctx = kv_malloc(sizeof(struct set_ctx));
    set_ctx_init(ctx, self, key, key_length, value, value_length, cb, cb_arg);
    ctx->bucket_index = kv_data_store_bucket_index(self, key);
    bucket_lock(self, ctx->bucket_index, set_lock_cb, ctx);
}
// --- get ---
struct get_ctx {
    struct kv_data_store *self;
    uint8_t *value;
    uint32_t *value_length;
    kv_data_store_cb cb;
    void *cb_arg;
    struct kv_bucket *buckets;
};
#define get_ctx_init(ctx, self, value, value_length, cb, cb_arg) \
    do {                                                         \
        ctx->self = self;                                        \
        ctx->value = value;                                      \
        ctx->value_length = value_length;                        \
        ctx->cb = cb;                                            \
        ctx->cb_arg = cb_arg;                                    \
    } while (0)

static void get_find_item_cb(bool success, struct kv_item *located_item, struct kv_bucket *located_bucket, void *cb_arg) {
    struct get_ctx *ctx = cb_arg;
    if (!success || !located_item) {
        if (!success) fprintf(stderr, "get_find_item_cb: IO error.\n");
        kv_storage_free(ctx->buckets);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    *(ctx->value_length) = located_item->value_length;
    kv_value_log_read(&ctx->self->value_log, located_item->value_offset, ctx->value, located_item->value_length, ctx->cb,
                      ctx->cb_arg);
    kv_storage_free(ctx->buckets);
    kv_free(ctx);
}

void kv_data_store_get(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t *value_length,
                       kv_data_store_cb cb, void *cb_arg) {
    uint32_t bucket_index = kv_data_store_bucket_index(self, key);
    struct get_ctx *ctx = kv_malloc(sizeof(struct get_ctx));
    get_ctx_init(ctx, self, value, value_length, cb, cb_arg);
    ctx->buckets = kv_storage_blk_alloc(self->bucket_log.log.storage, self->bucket_meta[bucket_index].chain_length);
    find_item(self, bucket_index, key, key_length, ctx->buckets, get_find_item_cb, ctx);
}