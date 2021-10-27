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
    kv_memset(self->compact_buffer, 0, sizeof(struct kv_bucket));

    uint32_t num_buckets = self->main_bucket_num + self->extra_bucket_num;
    self->bucket_offsets = kv_calloc(num_buckets, sizeof(struct kv_bucket_offset));
    kv_memset(self->bucket_offsets, 0, sizeof(struct kv_bucket_offset) * self->main_bucket_num);
    for (size_t i = 0; i < self->main_bucket_num; i++) {
        self->bucket_offsets[i].bucket_offset = self->bucket_log.log.head + i;
    }
    for (size_t i = self->main_bucket_num; i < num_buckets - 1; i++) {
        self->bucket_offsets[i].bucket_offset = i + 1;
    }
    self->bucket_offsets[num_buckets - 1] = (struct kv_bucket_offset){.flag = 0, .bucket_offset = 0};
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
            ctx->buckets[i].index = self->free_list_head;
        else
            ctx->buckets[i].index = self->bucket_log.tail + i - self->extra_bucket_num;
    }
    kv_bucket_log_write(&self->bucket_log, ctx->buckets, n, init_write_cb, ctx);
}

void kv_data_store_init(struct kv_data_store *self, struct kv_storage *storage, uint32_t base, uint32_t size,
                        kv_data_store_cb cb, void *cb_arg) {
    assert(base + size <= storage->num_blocks);
    uint32_t num_buckets = size / 8;
    size_t log_num_buckets = 0;
    while (((size_t)1 << log_num_buckets) < num_buckets) log_num_buckets++;
    assert(log_num_buckets <= 32);
    self->main_bucket_num = (size_t)1 << --log_num_buckets;

    self->buckets_mask = self->main_bucket_num - 1;
    self->extra_bucket_num = num_buckets - self->main_bucket_num;
    self->free_list_head = self->main_bucket_num;

    kv_bucket_log_init(&self->bucket_log, storage, base, num_buckets, 0, 0);
    kv_value_log_init(&self->value_log, storage, (base + num_buckets) * storage->block_size,
                      (size - num_buckets) * storage->block_size, 0, 0);

    struct init_ctx *ctx = kv_malloc(sizeof(struct init_ctx));
    ctx->self = self;
    ctx->cb = cb;
    ctx->cb_arg = cb_arg;
    ctx->io_num = INIT_CON_IO_NUM;
    for (size_t i = 0; i < INIT_CON_IO_NUM; i++) {
        struct init_write_ctx *buf = kv_storage_malloc(storage, sizeof(struct init_write_ctx));
        buf->init_ctx = ctx;
        init_write_cb(true, buf);
    }
}

void kv_data_store_fini(struct kv_data_store *self) {
    kv_free(self->bucket_offsets);
    kv_storage_free(self->compact_buffer);
    kv_bucket_log_fini(&self->bucket_log);
    kv_value_log_fini(&self->value_log);
}

// --- lock & unlock ---
static void bucket_lock(struct kv_data_store *self, uint32_t index, kv_task_cb cb, void *cb_arg) {
    assert(index < self->main_bucket_num);
    assert(cb);
    if (self->bucket_offsets[index].flag) {
        kv_waiting_map_put(&self->waiting_map, index, cb, cb_arg);
    } else {
        self->bucket_offsets[index].flag = 1;
        cb(cb_arg);
    }
}

static void bucket_unlock(struct kv_data_store *self, uint32_t index) {
    assert(index < self->main_bucket_num);
    assert(self->bucket_offsets[index].flag);
    struct kv_waiting_task task = kv_waiting_map_get(&self->waiting_map, index);
    if (task.cb)
        kv_app_send_msg(kv_app_get_index(), task.cb, task.cb_arg);
    else
        self->bucket_offsets[index].flag = 0;
}
#define kv_data_store_bucket_index(self, key) (*(uint32_t *)(key)&self->buckets_mask)
static inline bool compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len) {
    return key1_len == key2_len && !kv_memcmp8(key1, key2, key1_len);
}
#define has_extra_bucket(bucket) ((bucket->next_extra_bucket_index) != 0)
// --- compact ---
static void compact_writev_cb(bool success, void *arg) {
    struct kv_data_store *self = arg;
    if (!success) {
        fprintf(stderr, "compact_writev_cb: IO error!\n");
        return;
    }
    for (size_t i = 0; i < self->compact_iovcnt; i++) {
        struct kv_bucket *bucket = self->compact_iov[i].iov_base;
        struct kv_bucket_offset *offset = self->bucket_offsets + bucket->index;
        offset->flag = 0;
        offset->bucket_offset = (self->compact_offset + i) % self->bucket_log.log.size;
    }
    kv_bucket_log_move_head(&self->bucket_log, COMPACT_BUCKET_NUM);
    self->is_compact_task_running = false;
}
static void compact_read_cb(bool success, void *arg) {
    struct kv_data_store *self = arg;
    if (!success) {
        fprintf(stderr, "compact_read_cb: IO error!\n");
        return;
    }
    self->compact_iovcnt = 0;
    for (size_t i = 0; i < COMPACT_BUCKET_NUM; i++) {
        struct kv_bucket_offset *offset = self->bucket_offsets + self->compact_buffer[i].index;
        if (offset->bucket_offset == self->bucket_log.log.head + i && !offset->flag) {
            offset->flag = 1;
            self->compact_iov[self->compact_iovcnt++] = (struct iovec){self->compact_buffer + i, 1};
        }
    }
    // printf("compact rate:%lf\n", ((double)self->compact_iovcnt) / COMPACT_BUCKET_NUM);
    self->compact_offset = self->bucket_log.log.tail;
    kv_bucket_log_writev(&self->bucket_log, self->compact_iov, self->compact_iovcnt, compact_writev_cb, self);
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
        kv_app_send_msg(kv_app_get_index(), _compact, self);
    }
}
//--- find item ---

struct bucket_chain {
    struct kv_bucket bucket;
    bool is_modify;
    TAILQ_ENTRY(bucket_chain) next;
};
TAILQ_HEAD(bucket_chain_head, bucket_chain);
typedef void (*_find_item_cb)(struct kv_item *located_item, struct bucket_chain_head *buckets, void *cb_arg);
struct find_item_ctx {
    struct kv_data_store *self;
    uint32_t bucket_index;
    uint8_t *key;
    uint8_t key_length;
    _find_item_cb cb;
    void *cb_arg;
    struct bucket_chain_head buckets;
    uint32_t last_bucket_index; // for debuging
};
#define find_item_ctx_init(ctx, self, bucket_index, key, key_length, cb, cb_arg) \
    do {                                                                         \
        ctx->self = self;                                                        \
        ctx->bucket_index = bucket_index;                                        \
        ctx->key = key;                                                          \
        ctx->key_length = key_length;                                            \
        ctx->cb = cb;                                                            \
        ctx->cb_arg = cb_arg;                                                    \
    } while (0)

static void bucket_chain_free(struct bucket_chain_head *buckets) {
    struct bucket_chain *p;
    while ((p = TAILQ_FIRST(buckets))) {
        TAILQ_REMOVE(buckets, p, next);
        kv_storage_free(p);
    }
}
static void find_item_read_cb(bool success, void *arg) {
    struct find_item_ctx *ctx = arg;
    if (!success) {
        bucket_chain_free(&ctx->buckets);
        ctx->cb(NULL, NULL, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    struct kv_bucket *bucket = &TAILQ_LAST(&ctx->buckets, bucket_chain_head)->bucket;
    assert(ctx->last_bucket_index == bucket->index);
    for (struct kv_item *item = bucket->items; item - bucket->items < KV_ITEM_PER_BUCKET; ++item) {
        // if(KV_EMPTY_ITEM(item)) continue; // empty item key_length != ctx->key_length
        if (compare_keys(item->key, item->key_length, ctx->key, ctx->key_length)) {
            // job done!
            ctx->cb(item, &ctx->buckets, ctx->cb_arg);
            kv_free(ctx);
            return;
        }
    }
    if (has_extra_bucket(bucket)) {
        assert(bucket->next_extra_bucket_index >= ctx->self->main_bucket_num);
        assert(ctx->self->bucket_offsets[bucket->next_extra_bucket_index].flag);
        struct bucket_chain *_bucket = kv_storage_malloc(ctx->self->bucket_log.log.storage, sizeof(struct bucket_chain));
        _bucket->is_modify = false;
        TAILQ_INSERT_TAIL(&ctx->buckets, _bucket, next);
        ctx->last_bucket_index = bucket->next_extra_bucket_index;  // for debuging
        kv_bucket_log_read(&ctx->self->bucket_log, ctx->self->bucket_offsets[bucket->next_extra_bucket_index].bucket_offset,
                           &_bucket->bucket, 1, find_item_read_cb, ctx);
    } else {
        // item not found
        ctx->cb(NULL, &ctx->buckets, ctx->cb_arg);
        kv_free(ctx);
    }
}
static void find_item(struct kv_data_store *self, uint32_t bucket_index, uint8_t *key, uint8_t key_length, _find_item_cb cb,
                      void *cb_arg) {
    assert(cb);
    struct find_item_ctx *ctx = kv_malloc(sizeof(struct find_item_ctx));
    find_item_ctx_init(ctx, self, bucket_index, key, key_length, cb, cb_arg);
    TAILQ_INIT(&ctx->buckets);
    struct bucket_chain *bucket = kv_storage_malloc(ctx->self->bucket_log.log.storage, sizeof(struct bucket_chain));
    assert(bucket);
    bucket->is_modify = false;
    TAILQ_INSERT_TAIL(&ctx->buckets, bucket, next);
    ctx->last_bucket_index = ctx->bucket_index; // for debuging
    kv_bucket_log_read(&ctx->self->bucket_log, ctx->self->bucket_offsets[ctx->bucket_index].bucket_offset, &bucket->bucket, 1,
                       find_item_read_cb, ctx);
}
// --- find empty ---
static bool alloc_extra_bucket(struct kv_data_store *self, struct bucket_chain_head *buckets) {
    if (self->free_list_head == 0) return false;
    struct bucket_chain *p = kv_storage_zmalloc(self->bucket_log.log.storage, sizeof(struct bucket_chain));
    p->is_modify = true;
    p->bucket.index = self->free_list_head;
    self->bucket_offsets[self->free_list_head].flag = 1;
    self->free_list_head = self->bucket_offsets[self->free_list_head].bucket_offset;
    TAILQ_LAST(buckets, bucket_chain_head)->bucket.next_extra_bucket_index = p->bucket.index;
    TAILQ_LAST(buckets, bucket_chain_head)->is_modify = true;
    TAILQ_INSERT_TAIL(buckets, p, next);
    return true;
}
static struct kv_item *find_empty(struct kv_data_store *self, struct bucket_chain_head *buckets,
                                  struct bucket_chain **located_bucket) {
    struct bucket_chain *p;
    TAILQ_FOREACH(p, buckets, next) {
        for (struct kv_item *item = p->bucket.items; item - p->bucket.items < KV_ITEM_PER_BUCKET; ++item)
            if (KV_EMPTY_ITEM(item)) {
                *located_bucket = p;
                return item;
            }
    }
    if (alloc_extra_bucket(self, buckets)) {
        *located_bucket = TAILQ_LAST(buckets, bucket_chain_head);
        return (*located_bucket)->bucket.items;
    } else {
        *located_bucket = NULL;
        return NULL;
    }
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
    struct bucket_chain_head *buckets;
    struct iovec *iov;
    int iovcnt;
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
    for (int i = 0; success && i < ctx->iovcnt; ++i) {
        uint32_t bucket_index = ((struct kv_bucket *)ctx->iov[i].iov_base)->index;
        ctx->self->bucket_offsets[bucket_index].bucket_offset = ctx->tail;
        ctx->tail = (ctx->tail + 1) % ctx->self->bucket_log.log.size;
    }
    bucket_unlock(ctx->self, ctx->bucket_index);
    bucket_chain_free(ctx->buckets);
    kv_free(ctx->iov);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    compact(ctx->self);
    kv_free(ctx);
}

static void set_write_value_log_cb(bool success, void *arg) {
    struct set_ctx *ctx = arg;
    if (!success) {
        fprintf(stderr, "set_write_value_log_cb: IO error.\n");
        bucket_unlock(ctx->self, ctx->bucket_index);
        bucket_chain_free(ctx->buckets);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    struct bucket_chain *p;
    uint32_t i = 0;
    ctx->iovcnt = 0;
    TAILQ_FOREACH(p, ctx->buckets, next) if (p->is_modify) ctx->iovcnt++;
    ctx->iov = kv_calloc(ctx->iovcnt, sizeof(struct iovec));
    TAILQ_FOREACH(p, ctx->buckets, next) {
        if (p->is_modify) {
            ctx->iov[i].iov_base = &p->bucket;
            ctx->iov[i].iov_len = 1;
            ++i;
        }
    }
    ctx->tail = ctx->self->bucket_log.log.tail;
    kv_bucket_log_writev(&ctx->self->bucket_log, ctx->iov, ctx->iovcnt, set_bucket_log_writev_cb, ctx);
}

static void set_find_item_cb(struct kv_item *located_item, struct bucket_chain_head *buckets, void *cb_arg) {
    struct set_ctx *ctx = cb_arg;
    if (!buckets) {
        fprintf(stderr, "set_find_item_cb: IO error.\n");
        bucket_unlock(ctx->self, ctx->bucket_index);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    ctx->buckets = buckets;
    if (located_item) {  // update
        TAILQ_LAST(buckets, bucket_chain_head)->is_modify = true;
        located_item->value_length = ctx->value_length;
        located_item->value_offset =
            kv_value_log_write(&ctx->self->value_log, ctx->value, ctx->value_length, set_write_value_log_cb, ctx);
    } else {  // new
        struct bucket_chain *located_bucket;
        if ((located_item = find_empty(ctx->self, buckets, &located_bucket))) {
            located_bucket->is_modify = true;
            located_item->key_length = ctx->key_length;
            kv_memcpy(located_item->key, ctx->key, ctx->key_length);
            located_item->value_length = ctx->value_length;
            located_item->value_offset =
                kv_value_log_write(&ctx->self->value_log, ctx->value, ctx->value_length, set_write_value_log_cb, ctx);
        } else {
            fprintf(stderr, "set_find_item_cb: No more bucket available.\n");
            bucket_unlock(ctx->self, ctx->bucket_index);
            bucket_chain_free(buckets);
            if (ctx->cb) ctx->cb(false, ctx->cb_arg);
            kv_free(ctx);
        }
    }
}

static void set_lock_cb(void *arg) {
    struct set_ctx *ctx = arg;
    find_item(ctx->self, ctx->bucket_index, ctx->key, ctx->key_length, set_find_item_cb, ctx);
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
};
#define get_ctx_init(ctx, self, value, value_length, cb, cb_arg) \
    do {                                                         \
        ctx->self = self;                                        \
        ctx->value = value;                                      \
        ctx->value_length = value_length;                        \
        ctx->cb = cb;                                            \
        ctx->cb_arg = cb_arg;                                    \
    } while (0)

static void get_find_item_cb(struct kv_item *located_item, struct bucket_chain_head *buckets, void *cb_arg) {
    struct get_ctx *ctx = cb_arg;
    if (!buckets || !located_item) {
        if (!buckets)
            fprintf(stderr, "get_find_item_cb: IO error.\n");
        else
            bucket_chain_free(buckets);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    *(ctx->value_length) = located_item->value_length;
    kv_value_log_read(&ctx->self->value_log, located_item->value_offset, ctx->value, located_item->value_length, ctx->cb,
                      ctx->cb_arg);
    bucket_chain_free(buckets);
    kv_free(ctx);
}

void kv_data_store_get(struct kv_data_store *self, uint8_t *key, uint8_t key_length, uint8_t *value, uint32_t *value_length,
                       kv_data_store_cb cb, void *cb_arg) {
    uint32_t bucket_index = kv_data_store_bucket_index(self, key);
    struct get_ctx *ctx = kv_malloc(sizeof(struct get_ctx));
    get_ctx_init(ctx, self, value, value_length, cb, cb_arg);
    find_item(self, bucket_index, key, key_length, get_find_item_cb, ctx);
}