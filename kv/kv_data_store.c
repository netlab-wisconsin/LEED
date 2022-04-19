#include "kv_data_store.h"

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <sys/queue.h>

#include "kv_app.h"
#include "kv_memory.h"

// --- queue ---
struct ssd_q_entry {
    kv_task_cb fn;
    void *ctx;
    STAILQ_ENTRY(ssd_q_entry) entry;
};
struct ssd_q_head {
    uint32_t size, cap;
    STAILQ_HEAD(, ssd_q_entry) head;
};

enum ssd_q_type { SSD_Q_GET_0, SSD_Q_GET_1, SSD_Q_SET_0, SSD_Q_SET_1, SSD_Q_SET_2, SSD_Q_DEL_0, SSD_Q_DEL_1 };
static void ssd_q_init(struct kv_data_store *self) {
    double partion;
    uint32_t io_depth;
    FILE *f = fopen("ssd_q.conf", "r");
    if (fscanf(f, "%u", &io_depth) != 1) exit(-1);
    for (size_t i = 0; i < SSD_Q_NUM; i++) {
        if (fscanf(f, "%lf", &partion) != 1) partion = 0;
        struct ssd_q_head *queue = kv_malloc(sizeof(struct ssd_q_head));
        *queue = (struct ssd_q_head){0, (uint32_t)ceil(io_depth * partion)};
        STAILQ_INIT(&queue->head);
        self->ssd_q[i] = queue;
    }
    fclose(f);
}
static void ssd_q_fini(struct kv_data_store *self) {
    for (size_t i = 0; i < SSD_Q_NUM; i++) kv_free(self->ssd_q[i]);
}
static __thread uint64_t counter = 10000;
static inline void ssd_enqueue(struct kv_data_store *self, enum ssd_q_type q_type, kv_task_cb fn, void *ctx) {
    if (++counter % 100000 == 0) {
        uint32_t sum = 0;
        for (size_t i = 0; i < SSD_Q_NUM; i++) sum += ((struct ssd_q_head *)self->ssd_q[i])->size;
        printf("%u\n", sum);
    }
    assert(fn);
    struct ssd_q_head *queue = self->ssd_q[(int)q_type];
    if (queue->size >= queue->cap) {
        struct ssd_q_entry *entry = kv_malloc(sizeof(struct ssd_q_entry));
        *entry = (struct ssd_q_entry){fn, ctx};
        STAILQ_INSERT_TAIL(&queue->head, entry, entry);
    } else {
        queue->size++;
        fn(ctx);
    }
}
static inline void ssd_dequeue(struct kv_data_store *self, enum ssd_q_type q_type) {
    struct ssd_q_head *queue = self->ssd_q[(int)q_type];
    if (--queue->size < queue->cap) {
        if (!STAILQ_EMPTY(&queue->head)) {
            struct ssd_q_entry *entry = STAILQ_FIRST(&queue->head);
            queue->size++;
            STAILQ_REMOVE_HEAD(&queue->head, entry);
            entry->fn(entry->ctx);
            kv_free(entry);
        }
    }
}
// ----
struct queue_entry {
    struct kv_data_store *self;
    enum kv_ds_op op;
    kv_task_cb fn;
    void *ctx;
    kv_data_store_cb cb;
    void *cb_arg;
    STAILQ_ENTRY(queue_entry) entry;
};
STAILQ_HEAD(queue_head, queue_entry);

static void *enqueue(struct kv_data_store *self, enum kv_ds_op op, kv_task_cb fn, void *ctx, kv_data_store_cb cb,
                     void *cb_arg) {
    struct queue_entry *entry = kv_malloc(sizeof(struct queue_entry));
    *entry = (struct queue_entry){self, op, fn, ctx, cb, cb_arg};
    struct kv_ds_q_info q_info = self->ds_queue->q_info[self->ds_id];
    if (kv_ds_queue_find(&q_info, NULL, 1, kv_ds_op_cost(op))) {
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
    q_info.size -= kv_ds_op_cost(entry->op);
    while (!STAILQ_EMPTY((struct queue_head *)self->q)) {
        struct queue_entry *first = STAILQ_FIRST((struct queue_head *)self->q);
        if (kv_ds_queue_find(&q_info, NULL, 1, kv_ds_op_cost(first->op))) {
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
    ssd_q_init(self);
}

void kv_data_store_fini(struct kv_data_store *self) {
    kv_bucket_log_fini(&self->bucket_log);
    kv_value_log_fini(&self->value_log);
    kv_free(self->q);
    ssd_q_fini(self);
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
    struct kv_item *item_to_move = last_bucket->items;
    TAILQ_FOREACH(ce, &entry->buckets, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len && bucket != last_bucket; ++bucket)
            for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
                if (KV_EMPTY_ITEM(item)) {
                    while (KV_EMPTY_ITEM(item_to_move)) {
                        if (++item_to_move - last_bucket->items == KV_ITEM_PER_BUCKET) {
                            kv_bucket_free_extra(entry);
                            return;
                        }
                    }
                    *item = *item_to_move;
                    item_to_move->key_length = 0;
                };
    }
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
    uint32_t io_cnt;
    struct kv_bucket_pool *entry;
    uint64_t value_offset;
    bool success;
};

static void set_pool_put_cb(bool success, void *arg) {
    struct set_ctx *ctx = arg;
    kv_bucket_unlock(&ctx->self->bucket_log, &ctx->index_set);
    ssd_dequeue(ctx->self, SSD_Q_SET_2);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx);
}
static void set_pool_put(void *arg) {
    struct set_ctx *ctx = arg;
    kv_bucket_pool_put(&ctx->self->bucket_log, ctx->entry, true, set_pool_put_cb, ctx);
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
        ssd_enqueue(ctx->self, SSD_Q_SET_2, set_pool_put, ctx);
    } else {  // create
        if ((located_item = find_empty(ctx->self, ctx->entry))) {
            located_item->key_length = ctx->key_length;
            kv_memcpy(located_item->key, ctx->key, ctx->key_length);
            located_item->value_length = ctx->value_length;
            located_item->value_offset = ctx->value_offset;
            ssd_enqueue(ctx->self, SSD_Q_SET_2, set_pool_put, ctx);
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
    ssd_dequeue(ctx->self, SSD_Q_SET_1);
    set_lock_cb(ctx);
}

static void set_pool_get_cb(struct kv_bucket_pool *entry, void *arg) {
    struct set_ctx *ctx = arg;
    ctx->entry = entry;
    ssd_dequeue(ctx->self, SSD_Q_SET_0);
    set_lock_cb(ctx);
}

static void set_value_write(void *arg) {
    struct set_ctx *ctx = arg;
    ctx->value_offset = kv_value_log_offset(&ctx->self->value_log);
    kv_value_log_write(&ctx->self->value_log, ctx->bucket_index, ctx->value, ctx->value_length, set_write_value_log_cb, ctx);
}

static void set_pool_get(void *arg) {
    struct set_ctx *ctx = arg;
    kv_bucket_pool_get(&ctx->self->bucket_log, ctx->bucket_index, set_pool_get_cb, ctx);
}

static void set_start(void *arg) {
    struct set_ctx *ctx = arg;
    ctx->io_cnt = 3;
    ssd_enqueue(ctx->self, SSD_Q_SET_0, set_pool_get, ctx);
    ssd_enqueue(ctx->self, SSD_Q_SET_1, set_value_write, ctx);
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
    ctx->cb_arg = enqueue(self, KV_DS_SET, set_start, ctx, cb, cb_arg);
}
// --- get ---
struct get_ctx {
    struct kv_data_store *self;
    uint8_t *key;
    uint8_t key_length;
    uint8_t *value;
    uint32_t *value_length;
    uint64_t value_offset;
    kv_data_store_cb cb;
    void *cb_arg;
};

static void get_read_value_cb(bool success, void *arg) {
    struct get_ctx *ctx = arg;
    ssd_dequeue(ctx->self, SSD_Q_GET_1);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx);
}

static void get_read_value(void *arg) {
    struct get_ctx *ctx = arg;
    kv_value_log_read(&ctx->self->value_log, ctx->value_offset, ctx->value, *ctx->value_length, get_read_value_cb, ctx);
}

static void get_pool_get_cb(struct kv_bucket_pool *entry, void *arg) {
    struct get_ctx *ctx = arg;
    ssd_dequeue(ctx->self, SSD_Q_GET_0);
    if (entry == NULL) {
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    struct kv_item *located_item;
    find_item_plus(ctx->self, entry, ctx->key, ctx->key_length, &located_item);
    if (located_item == NULL) {
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_bucket_pool_put(&ctx->self->bucket_log, entry, false, NULL, NULL);
        kv_free(ctx);
    } else {
        *ctx->value_length = located_item->value_length;
        ctx->value_offset = located_item->value_offset;
        ssd_enqueue(ctx->self, SSD_Q_GET_1, get_read_value, ctx);
        kv_bucket_pool_put(&ctx->self->bucket_log, entry, false, NULL, NULL);
    }
}

static void get_pool_get(void *arg) {
    struct get_ctx *ctx = arg;
    uint32_t bucket_index = kv_data_store_bucket_index(ctx->self, ctx->key);
    kv_bucket_pool_get(&ctx->self->bucket_log, bucket_index, get_pool_get_cb, ctx);
}

static void get_read_bucket(void *arg) {
    struct get_ctx *ctx = arg;
    ssd_enqueue(ctx->self, SSD_Q_GET_0, get_pool_get, ctx);
}

void kv_data_store_get(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t *value_length,
                       kv_data_store_cb cb, void *cb_arg) {
    struct get_ctx *ctx = kv_malloc(sizeof(struct get_ctx));
    *ctx = (struct get_ctx){self, key, key_length, value, value_length};
    ctx->cb = dequeue;
    ctx->cb_arg = enqueue(self, KV_DS_GET, get_read_bucket, ctx, cb, cb_arg);
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
    uint32_t io_cnt;
    struct kv_bucket_pool *entry;
};

static void delete_pool_put_cb(bool success, void *arg) {
    struct delete_ctx *ctx = arg;
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
    ctx->cb_arg = enqueue(self, KV_DS_DEL, delete_lock, ctx, cb, cb_arg);
}