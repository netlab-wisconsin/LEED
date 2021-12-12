#include "kv_data_store.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>

#include "kv_app.h"
#include "memery_operation.h"

// --- queue ---
struct queue_entry {
    struct kv_data_store *self;
    enum { OP_SET = 4, OP_GET = 2, OP_DEL = 3 } op;
    kv_task_cb fn;
    void *ctx;
    kv_data_store_cb cb;
    void *cb_arg;
    STAILQ_ENTRY(queue_entry) entry;
};
STAILQ_HEAD(queue_head, queue_entry);
static void *enqueue(struct kv_data_store *self, uint32_t op, kv_task_cb fn, void *ctx, kv_data_store_cb cb, void *cb_arg) {
    struct queue_entry *entry = kv_malloc(sizeof(struct queue_entry));
    *entry = (struct queue_entry){self, op, fn, ctx, cb, cb_arg};
    if (self->q_size + op <= self->q_max) {
        self->q_size += op;
        kv_app_send(self->bucket_log.log.thread_index, fn, ctx);
    } else {
        STAILQ_INSERT_TAIL((struct queue_head *)self->q, entry, entry);
    }
    return entry;
}
static void dequeue(bool success, void *arg) {
    struct queue_entry *entry = arg;
    struct kv_data_store *self = entry->self;
    self->q_size -= entry->op;
    while (!STAILQ_EMPTY((struct queue_head *)self->q)) {
        struct queue_entry *first = STAILQ_FIRST((struct queue_head *)self->q);
        if (first->op + self->q_size <= self->q_max) {
            self->q_size += first->op;
            kv_app_send(self->bucket_log.log.thread_index, first->fn, first->ctx);
            STAILQ_REMOVE_HEAD((struct queue_head *)self->q, entry);
        } else
            break;
    }
    if (entry->cb) entry->cb(success, entry->cb_arg);
    kv_free(entry);
}

// --- init & fini ---

#define kv_data_store_bucket_index(self, key) (*(uint32_t *)(key) & ((self)->bucket_log.bucket_num - 1))
static inline bool compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len) {
    return key1_len == key2_len && !kv_memcmp8(key1, key2, key1_len);
}

void kv_data_store_init(struct kv_data_store *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets,
                        uint64_t value_log_block_num, uint32_t compact_buf_len, kv_data_store_cb cb, void *cb_arg) {
    num_buckets = num_buckets > compact_buf_len << 4 ? num_buckets : compact_buf_len << 4;
    kv_bucket_log_init(&self->bucket_log, storage, base, num_buckets, compact_buf_len, cb, cb_arg);
    kv_value_log_init(&self->value_log, storage, &self->bucket_log, (base + self->bucket_log.log.size) * storage->block_size,
                      value_log_block_num * storage->block_size, compact_buf_len);
    uint64_t value_log_size = self->value_log.log.size + self->value_log.index_log.size;
    if (self->bucket_log.log.size + value_log_size > storage->num_blocks) {
        fprintf(stderr, "kv_data_store_init: Not enough space.\n");
        exit(-1);
    }
    printf("bucket log size: %lf GB\n", ((double)self->bucket_log.log.size) * storage->block_size / (1 << 30));
    printf("value log size: %lf GB\n", ((double)value_log_size) * storage->block_size / (1 << 30));
    self->q_max = 512;
    self->q = kv_malloc(sizeof(struct queue_head));
    STAILQ_INIT((struct queue_head *)self->q);
}

void kv_data_store_fini(struct kv_data_store *self) {
    kv_bucket_log_fini(&self->bucket_log);
    kv_value_log_fini(&self->value_log);
    kv_free(self->q);
}

// --- debug ---
static void print_buckets(struct kv_bucket *buckets) __attribute__((unused));
static void print_buckets(struct kv_bucket *buckets) {
    printf("bucket index %u: \n", buckets->index);
    for (struct kv_bucket *bucket = buckets; bucket - buckets < buckets->chain_length; ++bucket) {
        printf("chain index %u: ", bucket->chain_index);
        for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
            if (KV_EMPTY_ITEM(item))
                printf("0 ");
            else
                printf("1 ");
        puts("");
    }
    puts("-----");
}

static inline void verify_buckets(struct kv_bucket *buckets, uint32_t index, uint8_t length) {
    assert(length);
    for (size_t i = 0; i < length; i++) {
        assert(buckets[i].chain_index == i);
        assert(buckets[i].chain_length == length);
        assert(buckets[i].index == index);
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
    verify_buckets(ctx->buckets, ctx->bucket_index,
                   kv_bucket_get_meta(&ctx->self->bucket_log, ctx->bucket_index)->chain_length);
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
    struct find_item_ctx *ctx = kv_malloc(sizeof(struct find_item_ctx));
    find_item_ctx_init(ctx, self, bucket_index, key, key_length, buckets, cb, cb_arg);
    kv_bucket_log_read(&self->bucket_log, bucket_index, buckets, find_item_read_cb, ctx);
}
// --- find empty ---
static bool alloc_extra_bucket(struct kv_data_store *self, struct kv_bucket *buckets) {
    uint8_t length = buckets->chain_length;
    if (length == 0x7F) return false;
    buckets[length].index = buckets->index;
    buckets[length].chain_index = length;
    length++;
    for (size_t i = 0; i < length; i++) buckets[i].chain_length = length;
    return true;
}

static void free_extra_bucket(struct kv_data_store *self, struct kv_bucket *buckets) {
    uint8_t chain_length = buckets->chain_length - 1;
    for (struct kv_bucket *bucket = buckets; bucket - buckets < chain_length; ++bucket) {
        bucket->chain_length = chain_length;
    }
}

static struct kv_item *find_empty(struct kv_data_store *self, struct kv_bucket *buckets) {
    for (struct kv_bucket *bucket = buckets; bucket - buckets < buckets->chain_length; ++bucket)
        for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
            if (KV_EMPTY_ITEM(item)) return item;
    if (alloc_extra_bucket(self, buckets)) return buckets[buckets->chain_length - 1].items;
    return NULL;
}

static void fill_the_hole(struct kv_data_store *self, struct kv_bucket *buckets) {
    if (buckets->chain_length == 1) return;
    struct kv_bucket *bucket = buckets;
    struct kv_item *item = bucket->items;
    for (size_t i = 0; i < KV_ITEM_PER_BUCKET; i++) {
        struct kv_item *item_to_move = buckets[buckets->chain_length - 1].items + i;
        if (KV_EMPTY_ITEM(item_to_move)) continue;
        for (; bucket - buckets < buckets->chain_length - 1; item = (++bucket)->items)
            for (; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
                if (KV_EMPTY_ITEM(item)) {
                    kv_memcpy(item, item_to_move, sizeof(struct kv_item));
                    item_to_move->key_length = 0;
                    goto find_next_item_to_move;
                }
        // no hole to fill
        return;
    find_next_item_to_move:;
    }
    // last bucket is empty
    free_extra_bucket(self, buckets);
}

// --- set ---
struct set_ctx {
    struct kv_data_store *self;
    uint8_t *key;
    uint8_t key_length;
    uint8_t *value;
    uint32_t value_length;
    kv_data_store_cb cb;
    void *cb_arg;
    uint32_t bucket_index;
    struct kv_bucket_lock_entry *index_set;
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

static void set_bucket_log_write_cb(bool success, void *arg) {
    struct set_ctx *ctx = arg;
    if (success) {
        struct kv_bucket_meta *meta = kv_bucket_get_meta(&ctx->self->bucket_log, ctx->bucket_index);
        meta->chain_length = ctx->buckets->chain_length;
        meta->bucket_offset = ctx->tail;
        verify_buckets(ctx->buckets, ctx->bucket_index, meta->chain_length);
    }
    kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
    kv_storage_free(ctx->buckets);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx);
}

static void set_write_value_log_cb(bool success, void *arg) {
    struct set_ctx *ctx = arg;
    if (success) {
        ctx->tail = kv_bucket_log_offset(&ctx->self->bucket_log);
        kv_bucket_log_write(&ctx->self->bucket_log, ctx->buckets, ctx->buckets->chain_length, set_bucket_log_write_cb, ctx);
    } else {
        fprintf(stderr, "set_write_value_log_cb: IO error.\n");
        kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
        kv_storage_free(ctx->buckets);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
    }
}

static void set_find_item_cb(bool success, struct kv_item *located_item, struct kv_bucket *located_bucket, void *cb_arg) {
    struct set_ctx *ctx = cb_arg;
    if (!success) {
        fprintf(stderr, "set_find_item_cb: IO error.\n");
        kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
        kv_storage_free(ctx->buckets);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    if (located_item) {  // update
        located_item->value_length = ctx->value_length;
        located_item->value_offset = kv_value_log_offset(&ctx->self->value_log);
        kv_value_log_write(&ctx->self->value_log, ctx->bucket_index, ctx->value, ctx->value_length, set_write_value_log_cb,
                           ctx);
    } else {  // new
        if ((located_item = find_empty(ctx->self, ctx->buckets))) {
            located_item->key_length = ctx->key_length;
            kv_memcpy(located_item->key, ctx->key, ctx->key_length);
            located_item->value_length = ctx->value_length;
            located_item->value_offset = kv_value_log_offset(&ctx->self->value_log);
            kv_value_log_write(&ctx->self->value_log, ctx->bucket_index, ctx->value, ctx->value_length, set_write_value_log_cb,
                               ctx);
        } else {
            fprintf(stderr, "set_find_item_cb: No more bucket available.\n");
            kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
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
    struct kv_bucket_meta *meta = kv_bucket_get_meta(&ctx->self->bucket_log, ctx->bucket_index);
    ctx->buckets = kv_storage_zblk_alloc(storage, meta->chain_length + 1);
    find_item(ctx->self, ctx->bucket_index, ctx->key, ctx->key_length, ctx->buckets, set_find_item_cb, ctx);
}

static void set_lock(void *arg) {
    struct set_ctx *ctx = arg;
    kv_bucket_lock_add_index(&ctx->index_set, ctx->bucket_index);
    kv_bucket_lock(&ctx->self->bucket_log, ctx->index_set, set_lock_cb, ctx);
}

void kv_data_store_set(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t value_length,
                       kv_data_store_cb cb, void *cb_arg) {
    struct set_ctx *ctx = kv_malloc(sizeof(struct set_ctx));
    set_ctx_init(ctx, self, key, key_length, value, value_length, cb, cb_arg);
    ctx->bucket_index = kv_data_store_bucket_index(self, key);
    ctx->index_set = NULL;
    ctx->cb = dequeue;
    ctx->cb_arg = enqueue(self, OP_SET, set_lock, ctx, cb, cb_arg);
}
// --- get ---
struct get_ctx {
    struct kv_data_store *self;
    uint8_t *key;
    uint8_t key_length;
    uint8_t *value;
    uint32_t *value_length;
    kv_data_store_cb cb;
    void *cb_arg;
    struct kv_bucket *buckets;
};

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

static void get_find_item(void *arg) {
    struct get_ctx *ctx = arg;
    uint32_t bucket_index = kv_data_store_bucket_index(ctx->self, ctx->key);
    struct kv_bucket_meta *meta = kv_bucket_get_meta(&ctx->self->bucket_log, bucket_index);
    ctx->buckets = kv_storage_blk_alloc(ctx->self->bucket_log.log.storage, meta->chain_length);
    find_item(ctx->self, bucket_index, ctx->key, ctx->key_length, ctx->buckets, get_find_item_cb, ctx);
}

void kv_data_store_get(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t *value_length,
                       kv_data_store_cb cb, void *cb_arg) {
    struct get_ctx *ctx = kv_malloc(sizeof(struct get_ctx));
    *ctx = (struct get_ctx){self, key, key_length, value, value_length};
    ctx->cb = dequeue;
    ctx->cb_arg = enqueue(self, OP_GET, get_find_item, ctx, cb, cb_arg);
}

// --- delete ---
struct delete_ctx {
    struct kv_data_store *self;
    uint8_t *key;
    uint8_t key_length;
    kv_data_store_cb cb;
    void *cb_arg;
    uint32_t bucket_index;
    struct kv_bucket_lock_entry *index_set;
    struct kv_bucket *buckets;
    uint32_t tail;
};
#define delete_ctx_init(ctx, self, key, key_length, cb, cb_arg) \
    do {                                                        \
        ctx->self = self;                                       \
        ctx->key = key;                                         \
        ctx->key_length = key_length;                           \
        ctx->cb = cb;                                           \
        ctx->cb_arg = cb_arg;                                   \
    } while (0)

static void delete_bucket_log_write_cb(bool success, void *arg) {
    struct delete_ctx *ctx = arg;
    if (success) {
        struct kv_bucket_meta *meta = kv_bucket_get_meta(&ctx->self->bucket_log, ctx->bucket_index);
        meta->chain_length = ctx->buckets->chain_length;
        meta->bucket_offset = ctx->tail;
        verify_buckets(ctx->buckets, ctx->bucket_index, meta->chain_length);
    }
    kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
    kv_storage_free(ctx->buckets);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx);
}

static void delete_find_item_cb(bool success, struct kv_item *located_item, struct kv_bucket *located_bucket, void *cb_arg) {
    struct delete_ctx *ctx = cb_arg;
    if (!success || !located_item) {
        if (!success) fprintf(stderr, "delete_find_item_cb: IO error.\n");
        kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
        kv_storage_free(ctx->buckets);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    located_item->key_length = 0;
    fill_the_hole(ctx->self, ctx->buckets);
    ctx->tail = kv_bucket_log_offset(&ctx->self->bucket_log);
    kv_bucket_log_write(&ctx->self->bucket_log, ctx->buckets, ctx->buckets->chain_length, delete_bucket_log_write_cb, ctx);
}

static void delete_lock_cb(void *arg) {
    struct delete_ctx *ctx = arg;
    struct kv_storage *storage = ctx->self->bucket_log.log.storage;
    struct kv_bucket_meta *meta = kv_bucket_get_meta(&ctx->self->bucket_log, ctx->bucket_index);
    ctx->buckets = kv_storage_blk_alloc(storage, meta->chain_length);
    find_item(ctx->self, ctx->bucket_index, ctx->key, ctx->key_length, ctx->buckets, delete_find_item_cb, ctx);
}

static void delete_lock(void *arg) {
    struct delete_ctx *ctx = arg;
    kv_bucket_lock_add_index(&ctx->index_set, ctx->bucket_index);
    kv_bucket_lock(&ctx->self->bucket_log, ctx->index_set, delete_lock_cb, ctx);
}

void kv_data_store_delete(struct kv_data_store *self, uint8_t *key, uint8_t key_length, kv_data_store_cb cb, void *cb_arg) {
    struct delete_ctx *ctx = kv_malloc(sizeof(struct delete_ctx));
    delete_ctx_init(ctx, self, key, key_length, cb, cb_arg);
    ctx->bucket_index = kv_data_store_bucket_index(self, key);
    ctx->index_set = NULL;
    ctx->cb = dequeue;
    ctx->cb_arg = enqueue(self, OP_DEL, delete_lock, ctx, cb, cb_arg);
}