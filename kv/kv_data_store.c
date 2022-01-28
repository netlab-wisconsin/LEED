#include "kv_data_store.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>

#include "kv_app.h"
#include "kv_memory.h"

// --- queue ---
struct queue_entry {
    struct kv_data_store *self;
    enum { OP_SET, OP_GET, OP_DEL } op;
    kv_task_cb fn;
    void *ctx;
    kv_data_store_cb cb;
    void *cb_arg;
    STAILQ_ENTRY(queue_entry) entry;
};
STAILQ_HEAD(queue_head, queue_entry);
static inline uint32_t op_cost(uint32_t op) {
    switch (op) {
        case OP_SET:
            return 10;
        case OP_GET:
            return 3;
        case OP_DEL:
            return 4;
        default:
            assert(false);
    }
    return 0;
}

static void *enqueue(struct kv_data_store *self, uint32_t op, kv_task_cb fn, void *ctx, kv_data_store_cb cb, void *cb_arg) {
    struct queue_entry *entry = kv_malloc(sizeof(struct queue_entry));
    struct kv_ds_q_info q_info = self->ds_queue->q_info[self->ds_id];
    *entry = (struct queue_entry){self, op, fn, ctx, cb, cb_arg};
    if (q_info.size + op <= q_info.cap) {
        q_info.size += op_cost(op);
        self->ds_queue->q_info[self->ds_id] = q_info;
        kv_app_send(self->bucket_log.log.thread_index, fn, ctx);
    } else {
        STAILQ_INSERT_TAIL((struct queue_head *)self->q, entry, entry);
    }
    return entry;
}
static void dequeue(bool success, void *arg) {
    struct queue_entry *entry = arg;
    struct kv_data_store *self = entry->self;
    struct kv_ds_q_info q_info = self->ds_queue->q_info[self->ds_id];
    q_info.size -= op_cost(entry->op);
    while (!STAILQ_EMPTY((struct queue_head *)self->q)) {
        struct queue_entry *first = STAILQ_FIRST((struct queue_head *)self->q);
        if (op_cost(first->op) + q_info.size <= q_info.cap) {
            q_info.size += op_cost(first->op);
            kv_app_send(self->bucket_log.log.thread_index, first->fn, first->ctx);
            STAILQ_REMOVE_HEAD((struct queue_head *)self->q, entry);
        } else
            break;
    }
    self->ds_queue->q_info[self->ds_id] = q_info;
    if (entry->cb) entry->cb(success, entry->cb_arg);
    kv_free(entry);
}

// --- init & fini ---

#define kv_data_store_bucket_index(self, key) (*(uint32_t *)(key) & ((self)->bucket_log.bucket_num - 1))
static inline bool compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len) {
    return key1_len == key2_len && !kv_memcmp8(key1, key2, key1_len);
}

void kv_data_store_init(struct kv_data_store *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets,
                        uint64_t value_log_block_num, uint32_t compact_buf_len, struct kv_ds_queue *ds_queue, uint32_t ds_id,
                        kv_data_store_cb cb, void *cb_arg) {
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
    self->ds_queue = ds_queue;
    self->ds_id = ds_id;
    self->ds_queue->q_info[self->ds_id] = (struct kv_ds_q_info){.cap = 1024, .size = 0};
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

static void find_item_plus(struct kv_data_store *self, struct kv_bucket_pool *entry, uint8_t *key, uint8_t key_length,
                           struct kv_item **located_item) {
    assert(located_item);
    struct kv_bucket_chain_entry *ce;
    TAILQ_FOREACH(ce, &entry->buckets, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket)
            for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item) {
                // if(KV_EMPTY_ITEM(item)) continue; // empty item key_length != ctx->key_length
                if (compare_keys(item->key, item->key_length, key, key_length)) {
                    // job done!
                    *located_item = item;
                    return;
                }
            }
    }
    *located_item = NULL;
}

// --- find empty ---
static struct kv_item *find_empty(struct kv_data_store *self, struct kv_bucket_pool *entry) {
    struct kv_bucket_chain_entry *ce;
    TAILQ_FOREACH(ce, &entry->buckets, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket)
            for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
                if (KV_EMPTY_ITEM(item)) return item;
    }
    if (kv_bucket_alloc_extra(entry)) {
        ce = TAILQ_LAST(&entry->buckets, kv_bucket_chain);
        return ce->bucket[ce->len - 1].items;
    }
    return NULL;
}

static void fill_the_hole(struct kv_data_store *self, struct kv_bucket_pool *entry) {
    if (TAILQ_FIRST(&entry->buckets)->bucket->chain_length == 1) return;

    struct kv_bucket_chain_entry *ce = TAILQ_LAST(&entry->buckets, kv_bucket_chain);
    struct kv_bucket *last_bucket = &ce->bucket[ce->len - 1];

    ce = TAILQ_FIRST(&entry->buckets);
    struct kv_bucket *bucket = ce->bucket;
    struct kv_item *item = bucket->items;
    for (size_t i = 0; i < KV_ITEM_PER_BUCKET; i++) {
        struct kv_item *item_to_move = last_bucket->items + i;
        if (KV_EMPTY_ITEM(item_to_move)) continue;
        while (true) {
            for (; bucket - ce->bucket < ce->len && bucket != last_bucket; item = (++bucket)->items)
                for (; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
                    if (KV_EMPTY_ITEM(item)) {
                        kv_memcpy(item, item_to_move, sizeof(struct kv_item));
                        item_to_move->key_length = 0;
                        goto find_next_item_to_move;
                    }
            ce = TAILQ_NEXT(ce, entry);
            if (ce == NULL) return;  // no hole to fill
            bucket = ce->bucket;
        }
    find_next_item_to_move:;
    }
    // last bucket is empty
    kv_bucket_free_extra(entry);
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
    uint32_t bucket_offset, io_cnt;
    struct kv_bucket_pool *entry;
    uint64_t value_offset;
    bool success;
    uint8_t chain_length;
};

static void set_pool_put_cb(bool success, void *arg) {
    struct set_ctx *ctx = arg;
    if (success) {
        struct kv_bucket_meta *meta = kv_bucket_get_meta(&ctx->self->bucket_log, ctx->bucket_index);
        meta->chain_length = ctx->chain_length;
        meta->bucket_offset = ctx->bucket_offset;
    }
    kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx);
}

static void set_lock_cb(void *arg) {
    struct set_ctx *ctx = arg;
    if (--ctx->io_cnt) return;  // sync
    if (ctx->entry == NULL || ctx->success == false) {
        if (ctx->entry == NULL)
            fprintf(stderr, "set_find_item_cb: IO error.\n");
        else
            kv_bucket_pool_put(&ctx->self->bucket_log, ctx->entry, false, NULL, NULL);
        if (ctx->success == false) fprintf(stderr, "set_write_value_log_cb: IO error.\n");
        kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    struct kv_item *located_item;
    find_item_plus(ctx->self, ctx->entry, ctx->key, ctx->key_length, &located_item);
    if (located_item) {  // update
        located_item->value_length = ctx->value_length;
        located_item->value_offset = ctx->value_offset;
        ctx->chain_length = TAILQ_FIRST(&ctx->entry->buckets)->bucket->chain_length;
        ctx->bucket_offset = kv_bucket_log_offset(&ctx->self->bucket_log);
        kv_bucket_pool_put(&ctx->self->bucket_log, ctx->entry, true, set_pool_put_cb, ctx);
    } else {  // create
        if ((located_item = find_empty(ctx->self, ctx->entry))) {
            located_item->key_length = ctx->key_length;
            kv_memcpy(located_item->key, ctx->key, ctx->key_length);
            located_item->value_length = ctx->value_length;
            located_item->value_offset = ctx->value_offset;
            ctx->chain_length = TAILQ_FIRST(&ctx->entry->buckets)->bucket->chain_length;
            ctx->bucket_offset = kv_bucket_log_offset(&ctx->self->bucket_log);
            kv_bucket_pool_put(&ctx->self->bucket_log, ctx->entry, true, set_pool_put_cb, ctx);
        } else {
            fprintf(stderr, "set_find_item_cb: No more bucket available.\n");
            kv_bucket_pool_put(&ctx->self->bucket_log, ctx->entry, false, NULL, NULL);
            kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
            if (ctx->cb) ctx->cb(false, ctx->cb_arg);
            kv_free(ctx);
        }
    }
}

static void set_write_value_log_cb(bool success, void *arg) {
    struct set_ctx *ctx = arg;
    ctx->success = success;
    set_lock_cb(ctx);
}

static void set_pool_get_cb(struct kv_bucket_pool *entry, void *arg) {
    struct set_ctx *ctx = arg;
    ctx->entry = entry;
    set_lock_cb(ctx);
}

static void set_start(void *arg) {
    struct set_ctx *ctx = arg;
    ctx->io_cnt = 3;
    kv_bucket_pool_get(&ctx->self->bucket_log, ctx->bucket_index, set_pool_get_cb, ctx);
    ctx->value_offset = kv_value_log_offset(&ctx->self->value_log);
    kv_value_log_write(&ctx->self->value_log, ctx->bucket_index, ctx->value, ctx->value_length, set_write_value_log_cb, ctx);
    kv_bucket_lock_add_index(&ctx->index_set, ctx->bucket_index);
    kv_bucket_lock(&ctx->self->bucket_log, ctx->index_set, set_lock_cb, ctx);
}

void kv_data_store_set(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t value_length,
                       kv_data_store_cb cb, void *cb_arg) {
    struct set_ctx *ctx = kv_malloc(sizeof(struct set_ctx));
    *ctx = (struct set_ctx){self, key, key_length, value, value_length, cb, cb_arg};
    ctx->bucket_index = kv_data_store_bucket_index(self, key);
    ctx->index_set = NULL;
    ctx->cb = dequeue;
    ctx->cb_arg = enqueue(self, OP_SET, set_start, ctx, cb, cb_arg);
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
};

static void get_pool_get_cb(struct kv_bucket_pool *entry, void *arg) {
    struct get_ctx *ctx = arg;
    if (entry == NULL) {
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    struct kv_item *located_item;
    find_item_plus(ctx->self, entry, ctx->key, ctx->key_length, &located_item);
    if (located_item == NULL) {
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
    } else {
        kv_value_log_read(&ctx->self->value_log, located_item->value_offset, ctx->value, located_item->value_length, ctx->cb,
                          ctx->cb_arg);
    }
    kv_bucket_pool_put(&ctx->self->bucket_log, entry, false, NULL, NULL);
    kv_free(ctx);
    return;
}

static void get_read_bucket(void *arg) {
    struct get_ctx *ctx = arg;
    uint32_t bucket_index = kv_data_store_bucket_index(ctx->self, ctx->key);
    kv_bucket_pool_get(&ctx->self->bucket_log, bucket_index, get_pool_get_cb, ctx);
}

void kv_data_store_get(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t *value_length,
                       kv_data_store_cb cb, void *cb_arg) {
    struct get_ctx *ctx = kv_malloc(sizeof(struct get_ctx));
    *ctx = (struct get_ctx){self, key, key_length, value, value_length};
    ctx->cb = dequeue;
    ctx->cb_arg = enqueue(self, OP_GET, get_read_bucket, ctx, cb, cb_arg);
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

    uint32_t bucket_offset, io_cnt;
    struct kv_bucket_pool *entry;
    uint8_t chain_length;
};
#define delete_ctx_init(ctx, self, key, key_length, cb, cb_arg) \
    do {                                                        \
        ctx->self = self;                                       \
        ctx->key = key;                                         \
        ctx->key_length = key_length;                           \
        ctx->cb = cb;                                           \
        ctx->cb_arg = cb_arg;                                   \
    } while (0)

static void delete_pool_put_cb(bool success, void *arg) {
    struct delete_ctx *ctx = arg;
    if (success) {
        struct kv_bucket_meta *meta = kv_bucket_get_meta(&ctx->self->bucket_log, ctx->bucket_index);
        meta->chain_length = ctx->chain_length;
        meta->bucket_offset = ctx->bucket_offset;
    }
    kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx);
}

static void delete_lock_cb(void *arg) {
    struct delete_ctx *ctx = arg;
    if (--ctx->io_cnt) return;  // sync
    if (ctx->entry == NULL) {
        fprintf(stderr, "delete_find_item_cb: IO error.\n");
        kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    struct kv_item *located_item;
    find_item_plus(ctx->self, ctx->entry, ctx->key, ctx->key_length, &located_item);
    located_item->key_length = 0;
    fill_the_hole(ctx->self, ctx->entry);
    ctx->chain_length = TAILQ_FIRST(&ctx->entry->buckets)->bucket->chain_length;
    ctx->bucket_offset = kv_bucket_log_offset(&ctx->self->bucket_log);
    kv_bucket_pool_put(&ctx->self->bucket_log, ctx->entry, true, delete_pool_put_cb, ctx);
}

static void delete_pool_get_cb(struct kv_bucket_pool *entry, void *arg) {
    struct delete_ctx *ctx = arg;
    ctx->entry = entry;
    delete_lock_cb(ctx);
}

static void delete_lock(void *arg) {
    struct delete_ctx *ctx = arg;
    ctx->io_cnt = 2;
    kv_bucket_pool_get(&ctx->self->bucket_log, ctx->bucket_index, delete_pool_get_cb, ctx);
    kv_bucket_lock_add_index(&ctx->index_set, ctx->bucket_index);
    kv_bucket_lock(&ctx->self->bucket_log, ctx->index_set, delete_lock_cb, ctx);
}

void kv_data_store_delete(struct kv_data_store *self, uint8_t *key, uint8_t key_length, kv_data_store_cb cb, void *cb_arg) {
    struct delete_ctx *ctx = kv_malloc(sizeof(struct delete_ctx));
    *ctx = (struct delete_ctx){self, key, key_length, cb, cb_arg};
    ctx->bucket_index = kv_data_store_bucket_index(self, key);
    ctx->index_set = NULL;
    ctx->cb = dequeue;
    ctx->cb_arg = enqueue(self, OP_DEL, delete_lock, ctx, cb, cb_arg);
}