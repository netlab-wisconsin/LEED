#include "kv_data_store.h"

#include <assert.h>
#include <stdio.h>
#include <sys/queue.h>

#include "kv_app.h"
#include "kv_memory.h"

// --- queue ---
struct queue_entry {
    struct kv_data_store *self;
    enum kv_ds_op op;
    kv_task_cb fn;
    void *ctx;
    kv_data_store_cb cb;
    void *cb_arg;
    STAILQ_ENTRY(queue_entry)
    entry;
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
static inline uint64_t kv_data_store_bucket_id(struct kv_data_store *self, uint8_t *key) {
    return *(uint64_t *)key >> (64 - self->log_bucket_num);
}

static inline bool compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len) {
    return key1_len == key2_len && !kv_memcmp8(key1, key2, key1_len);
}

void kv_data_store_init(struct kv_data_store *self, struct kv_storage *storage, uint64_t base, uint64_t num_buckets, uint64_t log_bucket_num,
                        uint64_t value_log_block_num, uint32_t compact_buf_len, struct kv_ds_queue *ds_queue, uint32_t ds_id) {
    self->log_bucket_num = log_bucket_num;
    kv_bucket_log_init(&self->bucket_log, storage, base, num_buckets);
    kv_value_log_init(&self->value_log, storage, &self->bucket_log, (base + self->bucket_log.log.size) * storage->block_size,
                      value_log_block_num * storage->block_size, compact_buf_len);
    uint64_t value_log_size = self->value_log.log.size + self->value_log.id_log_size;
    if (self->bucket_log.log.size + value_log_size > storage->num_blocks) {
        fprintf(stderr, "kv_data_store_init: Not enough space.\n");
        exit(-1);
    }
    printf("bucket log size: %lf GB\n", ((double)self->bucket_log.log.size) * storage->block_size / (1 << 30));
    printf("value log size: %lf GB\n", ((double)value_log_size) * storage->block_size / (1 << 30));
    self->ds_queue = ds_queue;
    self->ds_id = ds_id;
    self->ds_queue->q_info[self->ds_id] = (struct kv_ds_q_info){.cap = 1024, .size = 0};
    self->dirty_set = kv_bucket_key_set_init();
    self->q = kv_malloc(sizeof(struct queue_head));
    STAILQ_INIT((struct queue_head *)self->q);
}

void kv_data_store_fini(struct kv_data_store *self) {
    kv_bucket_log_fini(&self->bucket_log);
    kv_value_log_fini(&self->value_log);
    kv_bucket_key_set_fini(self->dirty_set);
    kv_free(self->q);
}

// --- debug ---
static void print_buckets(struct kv_bucket *buckets) __attribute__((unused));
static void print_buckets(struct kv_bucket *buckets) {
    printf("bucket id %lu: \n", (uint64_t)buckets->id);
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

static void find_item_plus(struct kv_data_store *self, struct kv_bucket_segment *seg, uint8_t *key, uint8_t key_length,
                           struct kv_item **located_item) {
    assert(located_item);
    struct kv_bucket_chain_entry *ce;
    TAILQ_FOREACH(ce, &seg->chain, entry) {
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
static struct kv_item *find_empty(struct kv_data_store *self, struct kv_bucket_segment *seg) {
    struct kv_bucket_chain_entry *ce;
    TAILQ_FOREACH(ce, &seg->chain, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket)
            for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
                if (KV_EMPTY_ITEM(item)) return item;
    }
    if (kv_bucket_alloc_extra(&self->bucket_log, seg)) {
        ce = TAILQ_LAST(&seg->chain, kv_bucket_chain);
        return ce->bucket[ce->len - 1].items;
    }
    return NULL;
}

static void fill_the_hole(struct kv_data_store *self, struct kv_bucket_segment *seg) {
    if (TAILQ_FIRST(&seg->chain)->bucket->chain_length == 1) return;

    struct kv_bucket_chain_entry *ce = TAILQ_LAST(&seg->chain, kv_bucket_chain);
    struct kv_bucket *last_bucket = &ce->bucket[ce->len - 1];
    struct kv_item *item_to_move = last_bucket->items;
    TAILQ_FOREACH(ce, &seg->chain, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len && bucket != last_bucket; ++bucket)
            for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item)
                if (KV_EMPTY_ITEM(item)) {
                    while (KV_EMPTY_ITEM(item_to_move)) {
                        if (++item_to_move - last_bucket->items == KV_ITEM_PER_BUCKET) {
                            kv_bucket_free_extra(seg);
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
    uint64_t bucket_id;
    uint32_t io_cnt;
    struct kv_bucket_segments segs;
    struct kv_bucket_segment seg;
    uint64_t value_offset;
    bool success;
};

void kv_data_store_set_commit(kv_data_store_ctx arg, bool success) {
    struct set_ctx *ctx = arg;
    if (success) kv_bucket_seg_commit(&ctx->self->bucket_log, &ctx->seg);
    kv_bucket_unlock(&ctx->self->bucket_log, &ctx->segs);
    kv_free(ctx);
}

static void set_finish_cb(bool success, void *arg) {
    struct set_ctx *ctx = arg;
    success = ctx->success && success;
    if (--ctx->io_cnt) return;  // sync
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    if (!success) kv_data_store_set_commit(arg, false);
}

static void set_lock_cb(void *arg) {
    struct set_ctx *ctx = arg;
    if (ctx->success == false) {
        set_finish_cb(false, arg);
        return;
    }
    struct kv_item *located_item;
    find_item_plus(ctx->self, &ctx->seg, ctx->key, ctx->key_length, &located_item);
    if (located_item) {  // update
        ctx->seg.dirty = true;
        located_item->value_length = ctx->value_length;
        located_item->value_offset = ctx->value_offset;
        kv_bucket_seg_put(&ctx->self->bucket_log, &ctx->seg, set_finish_cb, ctx);
    } else {  // create
        if ((located_item = find_empty(ctx->self, &ctx->seg))) {
            ctx->seg.dirty = true;
            located_item->key_length = ctx->key_length;
            kv_memcpy(located_item->key, ctx->key, ctx->key_length);
            located_item->value_length = ctx->value_length;
            located_item->value_offset = ctx->value_offset;
            kv_bucket_seg_put(&ctx->self->bucket_log, &ctx->seg, set_finish_cb, ctx);
        } else {
            fprintf(stderr, "set_find_item_cb: No more bucket available.\n");
            set_finish_cb(false, arg);
        }
    }
}

static void set_start(void *arg) {
    struct set_ctx *ctx = arg;
    ctx->io_cnt = 2;
    ctx->success = true;
    ctx->value_offset = kv_value_log_offset(&ctx->self->value_log);
    kv_value_log_write(&ctx->self->value_log, ctx->bucket_id, ctx->value, ctx->value_length, set_finish_cb, ctx);

    TAILQ_INIT(&ctx->segs);
    kv_bucket_seg_init(&ctx->seg, ctx->bucket_id);
    TAILQ_INSERT_HEAD(&ctx->segs, &ctx->seg, entry);
    kv_bucket_lock(&ctx->self->bucket_log, &ctx->segs, set_lock_cb, ctx);
}

kv_data_store_ctx kv_data_store_set(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t value_length,
                                    kv_data_store_cb cb, void *cb_arg) {
    struct set_ctx *ctx = kv_malloc(sizeof(struct set_ctx));
    *ctx = (struct set_ctx){self, key, key_length, value, value_length, cb, cb_arg};
    ctx->bucket_id = kv_data_store_bucket_id(self, key);
    ctx->cb = dequeue;
    ctx->cb_arg = enqueue(self, KV_DS_SET, set_start, ctx, cb, cb_arg);
    return ctx;
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
    struct kv_bucket_segment seg;
};

static void get_seg_cb(bool success, void *arg) {
    struct get_ctx *ctx = arg;
    if (success) {
        struct kv_item *located_item;
        find_item_plus(ctx->self, &ctx->seg, ctx->key, ctx->key_length, &located_item);
        if (located_item) {
            *ctx->value_length = located_item->value_length;
            kv_value_log_read(&ctx->self->value_log, located_item->value_offset, ctx->value, located_item->value_length, ctx->cb,
                              ctx->cb_arg);
        } else {
            success = false;
        }
    }
    if (!success && ctx->cb) ctx->cb(false, ctx->cb_arg);
    kv_bucket_seg_cleanup(&ctx->self->bucket_log, &ctx->seg);
    kv_free(ctx);
}

static void get_read_bucket(void *arg) {
    struct get_ctx *ctx = arg;
    kv_bucket_seg_init(&ctx->seg, kv_data_store_bucket_id(ctx->self, ctx->key));
    kv_bucket_seg_get(&ctx->self->bucket_log, &ctx->seg, true, get_seg_cb, ctx);
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
    uint64_t bucket_id;
    struct kv_bucket_segments segs;
    struct kv_bucket_segment seg;
    bool success;
};

void kv_data_store_del_commit(kv_data_store_ctx arg, bool success) {
    struct delete_ctx *ctx = arg;
    if (success) kv_bucket_seg_commit(&ctx->self->bucket_log, &ctx->seg);
    kv_bucket_unlock(&ctx->self->bucket_log, &ctx->segs);
    kv_free(ctx);
}

static void delete_finish_cb(bool success, void *arg) {
    struct delete_ctx *ctx = arg;
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    if (!success) kv_data_store_del_commit(arg, false);
}

static void delete_lock_cb(void *arg) {
    struct delete_ctx *ctx = arg;
    struct kv_item *located_item = NULL;
    find_item_plus(ctx->self, &ctx->seg, ctx->key, ctx->key_length, &located_item);
    if (!located_item) {
        delete_finish_cb(false, arg);
        return;
    }
    ctx->seg.dirty = true;
    located_item->key_length = 0;
    fill_the_hole(ctx->self, &ctx->seg);
    kv_bucket_seg_put(&ctx->self->bucket_log, &ctx->seg, delete_finish_cb, ctx);
}

static void delete_lock(void *arg) {
    struct delete_ctx *ctx = arg;
    TAILQ_INIT(&ctx->segs);
    kv_bucket_seg_init(&ctx->seg, ctx->bucket_id);
    TAILQ_INSERT_HEAD(&ctx->segs, &ctx->seg, entry);
    kv_bucket_lock(&ctx->self->bucket_log, &ctx->segs, delete_lock_cb, ctx);
}

kv_data_store_ctx kv_data_store_delete(struct kv_data_store *self, uint8_t *key, uint8_t key_length, kv_data_store_cb cb, void *cb_arg) {
    struct delete_ctx *ctx = kv_malloc(sizeof(struct delete_ctx));
    *ctx = (struct delete_ctx){self, key, key_length, cb, cb_arg};
    ctx->bucket_id = kv_data_store_bucket_id(self, key);
    ctx->cb = dequeue;
    ctx->cb_arg = enqueue(self, KV_DS_DEL, delete_lock, ctx, cb, cb_arg);
    return ctx;
}

// --- get_range ---

struct range_read_val_ctx;
struct range_ctx {
    struct kv_data_store *self;
    uint64_t start_id;
    uint64_t end_id;
    uint32_t buf_num;
    kv_data_store_range_cb get_buf;
    void *arg;
    kv_data_store_range_cb get_range_cb;
    void *cb_arg;
    uint64_t bucket_id;
    uint32_t iocnt, queue_size;
    TAILQ_HEAD(, range_read_val_ctx)
    queue;
};
struct range_seg_ctx {
    struct kv_bucket_segment seg;
    struct kv_bucket_segments segs;
    struct range_ctx *ctx;
    uint32_t item_num;
};

struct range_read_val_ctx {
    struct kv_data_store_get_range_buf buf;
    struct range_seg_ctx *seg_ctx;
    struct kv_item *item;
    TAILQ_ENTRY(range_read_val_ctx)
    entry;
};
static void range_scheduler(struct range_ctx *ctx);
static void range_read_val_cb(bool success, void *arg) {
    if (success == false) {
        fprintf(stderr, "kv_datastore: range_read_val failed.\n");
        exit(-1);
    }
    struct range_read_val_ctx *read_val = arg;
    struct range_seg_ctx *seg_ctx = read_val->seg_ctx;
    struct range_ctx *ctx = seg_ctx->ctx;
    ctx->get_range_cb(&read_val->buf, ctx->cb_arg);
    ctx->iocnt--;
    kv_free(read_val);
    if (--seg_ctx->item_num == 0) {
        kv_bucket_unlock(&ctx->self->bucket_log, &seg_ctx->segs);
        kv_free(seg_ctx);
    }
    range_scheduler(ctx);
}

static void range_consumer(struct range_ctx *ctx) {
    while (ctx->iocnt < ctx->buf_num) {
        struct range_read_val_ctx *read_val = TAILQ_FIRST(&ctx->queue);
        if (read_val == NULL) return;
        TAILQ_REMOVE(&ctx->queue, read_val, entry);
        ctx->iocnt++;
        ctx->get_buf(&read_val->buf, ctx->arg);
        struct kv_item *item = read_val->item;
        *read_val->buf.key_length = item->key_length;
        kv_memcpy(read_val->buf.key, item->key, item->key_length);
        *read_val->buf.value_length = item->value_length;
        kv_value_log_read(&ctx->self->value_log, item->value_offset, read_val->buf.value, item->value_length,
                          range_read_val_cb, read_val);
    }
}

static void range_lock_cb(void *arg) {
    struct range_seg_ctx *seg_ctx = arg;
    struct range_ctx *ctx = seg_ctx->ctx;
    assert(!TAILQ_EMPTY(&seg_ctx->seg.chain));
    struct kv_bucket_chain_entry *ce;
    TAILQ_FOREACH(ce, &seg_ctx->seg.chain, entry) {
        for (struct kv_bucket *bucket = ce->bucket; bucket - ce->bucket < ce->len; ++bucket)
            for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item) {
                if (KV_EMPTY_ITEM(item)) continue;
                struct range_read_val_ctx *read_val = kv_malloc(sizeof(*read_val));
                read_val->item = item;
                read_val->seg_ctx = seg_ctx;
                seg_ctx->item_num++;
                ctx->queue_size++;
                TAILQ_INSERT_TAIL(&ctx->queue, read_val, entry);
            }
    }
    range_scheduler(ctx);
}

static bool range_producer(struct range_ctx *ctx) {
    uint64_t id_space_size = 1ULL << ctx->self->log_bucket_num, len = (id_space_size + ctx->end_id - ctx->start_id) % id_space_size;
    for (; (id_space_size + ctx->bucket_id - ctx->start_id) % id_space_size < len; ctx->bucket_id = (ctx->bucket_id + ctx->start_id) % id_space_size) {
        struct kv_bucket_meta meta = kv_bucket_meta_get(&ctx->self->bucket_log, ctx->bucket_id);
        if (meta.chain_length == 0) continue;
        struct range_seg_ctx *seg_ctx = kv_malloc(sizeof(*seg_ctx));
        TAILQ_INIT(&seg_ctx->segs);
        kv_bucket_seg_init(&seg_ctx->seg, ctx->bucket_id);
        TAILQ_INSERT_TAIL(&seg_ctx->segs, &seg_ctx->seg, entry);
        seg_ctx->ctx = ctx;
        seg_ctx->item_num = 0;
        kv_bucket_lock(&ctx->self->bucket_log, &seg_ctx->segs, range_lock_cb, seg_ctx);
        return true;
    }
    return false;
}
static void range_scheduler(struct range_ctx *ctx) {
    bool finish = false;
    // consumer
    // get a item from the queue -> get buf ->read value-> get_range_cb -> unlock
    range_consumer(ctx);
    // producer
    //  find a none empty bucket -> lock the bucket -> add items to the queue
    if (ctx->queue_size < ctx->buf_num) {
        finish = !range_producer(ctx);
    }

    if (finish && ctx->iocnt == 0) {
        // get_range finish
        ctx->get_range_cb(NULL, ctx->cb_arg);
        kv_free(ctx);
    }
}

void kv_data_store_get_range(struct kv_data_store *self, uint8_t *start_key, uint8_t *end_key, uint64_t buf_num,
                             kv_data_store_range_cb get_buf, void *arg, kv_data_store_range_cb get_range_cb, void *cb_arg) {
    assert(get_range_cb && get_buf);
    uint64_t start_id = kv_data_store_bucket_id(self, start_key), end_id = kv_data_store_bucket_id(self, end_key);
    struct range_ctx *ctx = kv_malloc(sizeof(*ctx));
    *ctx = (struct range_ctx){self, start_id, end_id, buf_num, get_buf, arg, get_range_cb, cb_arg};
    ctx->bucket_id = ctx->start_id;
    ctx->iocnt = 0;
    ctx->queue_size = 0;
    TAILQ_INIT(&ctx->queue);
    range_scheduler(ctx);
}

void kv_data_store_del_range(struct kv_data_store *self, uint8_t *start_key, uint8_t *end_key) {
    uint64_t start_id = kv_data_store_bucket_id(self, start_key), end_id = kv_data_store_bucket_id(self, end_key);
    uint64_t id_space_size = 1ULL << self->log_bucket_num, len = (id_space_size + end_id - start_id) % id_space_size;
    for (uint64_t i = start_id; (id_space_size + i - start_id) % id_space_size < len; i = (i + start_id) % id_space_size) {
        kv_bucket_meta_put(&self->bucket_log, i, (struct kv_bucket_meta){0, 0});
    }
}