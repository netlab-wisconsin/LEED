#include "kv_table.h"

#include <assert.h>
#include <stdio.h>

#include "kv_table_concurrent.h"
#include "memery_operation.h"

void mehcached_print_bucket(const struct mehcached_bucket *bucket) {
    printf("<bucket %zx>\n", (size_t)bucket);
    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        printf("  item_vec[%zu]: tag=%lu, item_offset=%lu\n", item_index, MEHCACHED_ITEM_TAG(bucket->item_vec[item_index]),
               MEHCACHED_ITEM_OFFSET(bucket->item_vec[item_index]));
}

void mehcached_print_buckets(const struct mehcached_table *table) {
    size_t bucket_index;
    for (bucket_index = 0; bucket_index < table->num_buckets; bucket_index++) {
        struct mehcached_bucket *bucket = table->buckets + bucket_index;
        mehcached_print_bucket(bucket);
    }
    printf("\n");
}

#define mehcached_has_extra_bucket(bucket) ((bucket->next_extra_bucket_index) != 0)

static struct mehcached_bucket *mehcached_extra_bucket(const struct mehcached_table *table, uint32_t extra_bucket_index) {
    // extra_bucket_index is 1-base
    assert(extra_bucket_index != 0);
    assert(extra_bucket_index < 1 + table->num_extra_buckets);
    return table->extra_buckets + extra_bucket_index;  // extra_buckets[1] is the actual start
}

static bool mehcached_alloc_extra_bucket(struct mehcached_table *table, struct mehcached_bucket *bucket) {
    assert(!mehcached_has_extra_bucket(bucket));

    mehcached_lock_extra_bucket_free_list(table);

    if (table->extra_bucket_free_list.head == 0) {
        mehcached_unlock_extra_bucket_free_list(table);
        return false;
    }

    // take the first free extra bucket
    uint32_t extra_bucket_index = table->extra_bucket_free_list.head;
    table->extra_bucket_free_list.head = mehcached_extra_bucket(table, extra_bucket_index)->next_extra_bucket_index;
    mehcached_extra_bucket(table, extra_bucket_index)->next_extra_bucket_index = 0;

    // add it to the given bucket
    // concurrent readers may see the new extra_bucket from this point
    bucket->next_extra_bucket_index = extra_bucket_index;

    mehcached_unlock_extra_bucket_free_list(table);
    return true;
}

static void mehcached_free_extra_bucket(struct mehcached_table *table, struct mehcached_bucket *bucket) {
    assert(mehcached_has_extra_bucket(bucket));

    uint32_t extra_bucket_index = bucket->next_extra_bucket_index;

    struct mehcached_bucket *extra_bucket = mehcached_extra_bucket(table, extra_bucket_index);
    assert(extra_bucket->next_extra_bucket_index == 0);  // only allows freeing the tail of the extra bucket chain

    // verify if the extra bucket is empty (debug only)
    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++) assert(extra_bucket->item_vec[item_index] == 0);

    // detach
    bucket->next_extra_bucket_index = 0;

    // add freed extra bucket to the free list
    mehcached_lock_extra_bucket_free_list(table);

    extra_bucket->next_extra_bucket_index = table->extra_bucket_free_list.head;
    table->extra_bucket_free_list.head = extra_bucket_index;

    mehcached_unlock_extra_bucket_free_list(table);
}

static void mehcached_fill_hole(struct mehcached_table *table, struct mehcached_bucket *bucket, size_t unused_item_index) {
    // there is no extra bucket; do not try to fill a hole within the same bucket
    if (!mehcached_has_extra_bucket(bucket)) return;

    struct mehcached_bucket *prev_extra_bucket = NULL;
    struct mehcached_bucket *current_extra_bucket = bucket;
    while (mehcached_has_extra_bucket(current_extra_bucket) != 0) {
        prev_extra_bucket = current_extra_bucket;
        current_extra_bucket = mehcached_extra_bucket(table, current_extra_bucket->next_extra_bucket_index);
    }

    bool last_item = true;
    size_t moved_item_index;

    size_t item_index;
    for (item_index = 0; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        if (current_extra_bucket->item_vec[item_index] != 0) {
            moved_item_index = item_index;
            break;
        }

    for (item_index++; item_index < MEHCACHED_ITEMS_PER_BUCKET; item_index++)
        if (current_extra_bucket->item_vec[item_index] != 0) {
            last_item = false;
            break;
        }

    // move the entry
    bucket->item_vec[unused_item_index] = current_extra_bucket->item_vec[moved_item_index];
    current_extra_bucket->item_vec[moved_item_index] = 0;

    // if it was the last entry of current_extra_bucket, free current_extra_bucket
    if (last_item) mehcached_free_extra_bucket(table, prev_extra_bucket);
}

static uint64_t *mehcached_find_empty(struct mehcached_table *self, struct mehcached_bucket *bucket) {
    while (true) {
        for (uint64_t *p = bucket->item_vec; p != bucket->item_vec + MEHCACHED_ITEMS_PER_BUCKET; ++p)
            if (MEHCACHED_ITEM_EMPTY(*p)) return p;
        if (!mehcached_has_extra_bucket(bucket)) break;
        bucket = mehcached_extra_bucket(self, bucket->next_extra_bucket_index);
    }
    // no space; alloc new extra_bucket
    if (mehcached_alloc_extra_bucket(self, bucket))
        return mehcached_extra_bucket(self, bucket->next_extra_bucket_index)->item_vec;
    else
        return NULL;
}

static bool mehcached_compare_keys(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len) {
    return key1_len == key2_len && kv_memcmp8(key1, key2, key1_len);
}

//--- find_item ---
typedef void (*_find_item_cb)(uint64_t *located_item, struct mehcached_bucket *located_bucket, void *cb_arg);
struct mehcached_find_item_context {
    const struct mehcached_table *self;
    struct mehcached_bucket *bucket;
    uint16_t tag;
    const uint8_t *key;
    size_t key_length;
    struct kv_log_header *header;
    _find_item_cb cb;
    void *cb_arg;
    uint64_t *item;
    bool alloc_header;
};

#define mehcached_find_item_context_init(ctx, self, bucket, tag, key, key_length, header, cb, cb_arg) \
    do {                                                                                              \
        ctx->self = self;                                                                             \
        ctx->bucket = bucket;                                                                         \
        ctx->tag = tag;                                                                               \
        ctx->key = key;                                                                               \
        ctx->key_length = key_length;                                                                 \
        ctx->header = header;                                                                         \
        ctx->cb = cb;                                                                                 \
        ctx->cb_arg = cb_arg;                                                                         \
    } while (0)

static void mehcached_find_item_fini(bool success, struct mehcached_find_item_context *ctx) {
    if (success)
        ctx->cb(ctx->item, ctx->bucket, ctx->cb_arg);
    else
        ctx->cb(NULL, NULL, ctx->cb_arg);
    if (ctx->alloc_header) kv_free(ctx->header);
    kv_free(ctx);
}
static void _mehcached_find_item(void *_arg);
static void find_item_read_cb(bool success, void *cb_arg) {
    struct mehcached_find_item_context *ctx = cb_arg;
    if (!success) {
        fprintf(stderr, "find_item_read_cb: io error.\n");
        mehcached_find_item_fini(false, ctx);
    } else if (mehcached_compare_keys(KV_LOG_KEY(ctx->header), ctx->header->key_length, ctx->key, ctx->key_length)) {
        ++ctx->item;
        _mehcached_find_item(ctx);
    } else
        mehcached_find_item_fini(true, ctx);
}

static void _mehcached_find_item(void *_arg) {
    struct mehcached_find_item_context *ctx = _arg;
    while (true) {
        for (; ctx->item != ctx->bucket->item_vec + MEHCACHED_ITEMS_PER_BUCKET; ++ctx->item) {
            if (MEHCACHED_ITEM_EMPTY(*ctx->item) || MEHCACHED_ITEM_TAG(*ctx->item) != ctx->tag) continue;
            kv_log_read_header(ctx->self->log, MEHCACHED_ITEM_OFFSET(*ctx->item), ctx->header, find_item_read_cb, ctx);
            return;
        }
        if (!mehcached_has_extra_bucket(ctx->bucket)) break;
        ctx->bucket = mehcached_extra_bucket(ctx->self, ctx->bucket->next_extra_bucket_index);
        ctx->item = ctx->bucket->item_vec;
    }
    mehcached_find_item_fini(false, ctx);
}

static void mehcached_find_item(const struct mehcached_table *self, struct mehcached_bucket *bucket, uint16_t tag, uint8_t *key,
                                size_t key_length, struct kv_log_header *header, _find_item_cb cb, void *cb_arg) {
    assert(cb);
    struct mehcached_find_item_context *ctx = kv_malloc(sizeof(struct mehcached_find_item_context));
    if ((ctx->alloc_header = (header == NULL))) header = kv_malloc(self->log->storage->block_size);
    mehcached_find_item_context_init(ctx, self, bucket, tag, key, key_length, header, cb, cb_arg);
    ctx->item = ctx->bucket->item_vec;
    _mehcached_find_item(ctx);
}

//--- set ---
struct mehcached_set_context {
    struct mehcached_table *self;
    uint64_t key_hash;
    uint8_t *key;
    size_t key_length;
    uint8_t *value;
    size_t value_length;
    kv_table_op_cb cb;
    void *cb_arg;
    uint16_t tag;
    struct mehcached_bucket *bucket;
    uint64_t *item;
    uint64_t item_offset;
};

#define mehcached_set_context_init(ctx, self, key_hash, key, key_length, value, value_length, cb, cb_arg) \
    do {                                                                                                  \
        ctx->self = self;                                                                                 \
        ctx->key_hash = key_hash;                                                                         \
        ctx->key = key;                                                                                   \
        ctx->key_length = key_length;                                                                     \
        ctx->value = value;                                                                               \
        ctx->value_length = value_length;                                                                 \
        ctx->cb = cb;                                                                                     \
        ctx->cb_arg = cb_arg;                                                                             \
    } while (0)

static void set_write_cb(bool success, void *cb_arg) {
    struct mehcached_set_context *ctx = cb_arg;
    if (!success)
        fprintf(stderr, "set_write_cb: write io erorr\n");
    else
        *(ctx->item) = MEHCACHED_ITEM_VEC(mehcached_calc_tag(ctx->key_hash), ctx->item_offset);
    mehcached_unlock_bucket(ctx->self, ctx->bucket);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx);
}

static void set_find_item_cb(uint64_t *located_item, struct mehcached_bucket *located_bucket, void *cb_arg) {
    struct mehcached_set_context *ctx = cb_arg;
    if (!located_item)
        if (!(located_item = mehcached_find_empty(ctx->self, ctx->bucket))) {
            fprintf(stderr, "no more space\n");
            mehcached_unlock_bucket(ctx->self, ctx->bucket);
            if (ctx->cb) ctx->cb(false, ctx->cb_arg);
            kv_free(ctx);
            return;
        }
    ctx->item_offset = ctx->self->log->tail;
    ctx->item = located_item;
    kv_log_write(ctx->self->log, ctx->item_offset, ctx->key, ctx->key_length, ctx->value, ctx->value_length, set_write_cb, ctx);
}

void mehcached_set(struct mehcached_table *self, uint64_t key_hash, uint8_t *key, size_t key_length, uint8_t *value,
                   size_t value_length, kv_table_op_cb cb, void *cb_arg) {
    struct mehcached_set_context *ctx = kv_malloc(sizeof(struct mehcached_set_context));
    mehcached_set_context_init(ctx, self, key_hash, key, key_length, value, value_length, cb, cb_arg);
    ctx->tag = mehcached_calc_tag(ctx->key_hash);
    ctx->bucket = self->buckets + mehcached_calc_bucket_index(self, key_hash);
    mehcached_lock_bucket(self, ctx->bucket);
    mehcached_find_item(self, ctx->bucket, ctx->tag, key, key_length, NULL, set_find_item_cb, ctx);
}

// --- delete ---
struct mehcached_delete_context {
    struct mehcached_table *self;
    uint64_t key_hash;
    uint8_t *key;
    size_t key_length;
    kv_table_op_cb cb;
    void *cb_arg;
    struct mehcached_bucket *bucket;
    struct mehcached_bucket *located_bucket;
    uint64_t *item;
};

#define mehcached_delete_context_init(ctx, self, key_hash, key, key_length, cb, cb_arg) \
    do {                                                                                \
        ctx->self = self;                                                               \
        ctx->key_hash = key_hash;                                                       \
        ctx->key = key;                                                                 \
        ctx->key_length = key_length;                                                   \
        ctx->cb = cb;                                                                   \
        ctx->cb_arg = cb_arg;                                                           \
    } while (0)

static void delete_delete_cb(bool success, void *cb_arg) {
    struct mehcached_delete_context *ctx = cb_arg;
    if (!success)
        fprintf(stderr, "delete_delete_cb: io erorr\n");
    else {
        *(ctx->item) = MEHCACHED_ITEM_INIT_VAL;
        mehcached_fill_hole(ctx->self, ctx->located_bucket, ctx->item - ctx->located_bucket->item_vec);
    }
    mehcached_unlock_bucket(ctx->self, ctx->bucket);
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx);
}

static void delete_find_item_cb(uint64_t *located_item, struct mehcached_bucket *located_bucket, void *cb_arg) {
    struct mehcached_delete_context *ctx = cb_arg;
    if (!located_item) {
        fprintf(stderr, "Delete not found\n");
        mehcached_unlock_bucket(ctx->self, ctx->bucket);
        if (ctx->cb) ctx->cb(false, ctx->cb_arg);
        kv_free(ctx);
        return;
    }
    ctx->item = located_item;
    ctx->located_bucket = located_bucket;
    kv_log_delete(ctx->self->log, ctx->self->log->tail, ctx->key, ctx->key_length, delete_delete_cb, ctx);
}

void mehcached_delete(struct mehcached_table *self, uint64_t key_hash, uint8_t *key, size_t key_length, kv_table_op_cb cb,
                      void *cb_arg) {
    struct mehcached_delete_context *ctx = kv_malloc(sizeof(struct mehcached_delete_context));
    mehcached_delete_context_init(ctx, self, key_hash, key, key_length, cb, cb_arg);
    ctx->bucket = self->buckets + mehcached_calc_bucket_index(self, key_hash);
    mehcached_lock_bucket(self, ctx->bucket);
    mehcached_find_item(self, ctx->bucket, mehcached_calc_tag(ctx->key_hash), key, key_length, NULL, delete_find_item_cb, ctx);
}
// --- read ---
struct mehcached_get_context {
    struct mehcached_table *self;
    uint64_t key_hash;
    uint8_t *key;
    size_t key_length;
    uint8_t *value;
    size_t *value_length;
    kv_table_op_cb cb;
    void *cb_arg;
    struct kv_log_header *header;
    uint16_t tag;
    struct mehcached_bucket *bucket;
    uint32_t version_start;
};
#define mehcached_get_context_init(ctx, self, key_hash, key, key_length, value, value_length, cb, cb_arg) \
    do {                                                                                                  \
        ctx->self = self;                                                                                 \
        ctx->key_hash = key_hash;                                                                         \
        ctx->key = key;                                                                                   \
        ctx->key_length = key_length;                                                                     \
        ctx->value = value;                                                                               \
        ctx->value_length = value_length;                                                                 \
        ctx->cb = cb;                                                                                     \
        ctx->cb_arg = cb_arg;                                                                             \
    } while (0)
static void _mehcached_get(void *arg);
static void get_read_value_cb(bool success, void *arg) {
    struct mehcached_get_context *ctx = arg;
    if (ctx->version_start != mehcached_read_version_end(ctx->self, ctx->bucket)) {
        _mehcached_get(ctx);
        return;
    }
    if (success)
        *(ctx->value_length) = ctx->header->value_length;
    else
        fprintf(stderr, "get_read_value_cb: io erorr\n");
    if (ctx->cb) ctx->cb(success, ctx->cb_arg);
    kv_free(ctx->header);
    kv_free(ctx);
}

static void get_find_item_cb(uint64_t *located_item, struct mehcached_bucket *located_bucket, void *cb_arg) {
    struct mehcached_get_context *ctx = cb_arg;
    if (!located_item) {
        if (ctx->version_start != mehcached_read_version_end(ctx->self, ctx->bucket))
            _mehcached_get(ctx);
        else {
            // not_found
            if (ctx->cb) ctx->cb(false, ctx->cb_arg);
            kv_free(ctx->header);
            kv_free(ctx);
        }
    } else {
        uint64_t offset = MEHCACHED_ITEM_OFFSET(*located_item);
        kv_log_read_value(ctx->self->log, offset, ctx->header, ctx->value, get_read_value_cb, ctx);
    }
}
static void _mehcached_get(void *arg) {
    struct mehcached_get_context *ctx = arg;
    ctx->version_start = mehcached_read_version_begin(ctx->self, ctx->bucket);
    mehcached_find_item(ctx->self, ctx->bucket, ctx->tag, ctx->key, ctx->key_length, ctx->header, get_find_item_cb, ctx);
}

void mehcached_get(struct mehcached_table *self, uint64_t key_hash, uint8_t *key, size_t key_length, uint8_t *value,
                   size_t *value_length, kv_table_op_cb cb, void *cb_arg) {
    struct mehcached_get_context *ctx = kv_malloc(sizeof(struct mehcached_get_context));
    mehcached_get_context_init(ctx, self, key_hash, key, key_length, value, value_length, cb, cb_arg);
    ctx->header = kv_malloc(self->log->storage->block_size);
    ctx->tag = mehcached_calc_tag(ctx->key_hash);
    ctx->bucket = self->buckets + mehcached_calc_bucket_index(self, key_hash);
    _mehcached_get(ctx);
}

void mehcached_table_reset(struct mehcached_table *self) {
    for (size_t i = 0; i < self->num_buckets + self->num_extra_buckets; i++) {
        self->buckets[i].version = 0;
        self->buckets[i].next_extra_bucket_index = 0;
        kv_memset(self->buckets[i].item_vec, 0xFF, sizeof(uint64_t) * MEHCACHED_ITEMS_PER_BUCKET);
    }

    // initialize a free list of extra buckets
    if (self->num_extra_buckets == 0)
        self->extra_bucket_free_list.head = 0;  // no extra bucket at all
    else {
        uint32_t extra_bucket_index;
        for (extra_bucket_index = 1; extra_bucket_index < 1 + self->num_extra_buckets - 1; extra_bucket_index++)
            (self->extra_buckets + extra_bucket_index)->next_extra_bucket_index = extra_bucket_index + 1;
        (self->extra_buckets + extra_bucket_index)->next_extra_bucket_index = 0;  // no more free extra bucket

        self->extra_bucket_free_list.head = 1;
    }
}

void mehcached_table_init(struct mehcached_table *self, struct kv_log *log, size_t num_buckets, bool concurrent_table_read,
                          uint8_t extra_buckets_percentage) {
    assert((MEHCACHED_ITEMS_PER_BUCKET == 7 && sizeof(struct mehcached_bucket) == 64) ||
           (MEHCACHED_ITEMS_PER_BUCKET == 15 && sizeof(struct mehcached_bucket) == 128) ||
           (MEHCACHED_ITEMS_PER_BUCKET == 31 && sizeof(struct mehcached_bucket) == 256));
    assert(log);

    self->log = log;

    size_t log_num_buckets = 0;
    while (((size_t)1 << log_num_buckets) < num_buckets) log_num_buckets++;
    num_buckets = (size_t)1 << log_num_buckets;
    assert(log_num_buckets <= 32);

    self->concurrent_access_mode = (uint8_t)concurrent_table_read;

    self->num_buckets = (uint32_t)num_buckets;
    self->num_buckets_mask = (uint32_t)num_buckets - 1;
    self->num_extra_buckets = self->num_buckets * extra_buckets_percentage / 100;

    self->buckets = kv_calloc(self->num_buckets + self->num_extra_buckets, sizeof(struct mehcached_bucket));
    self->extra_buckets = self->buckets + self->num_buckets - 1;  // subtract by one to compensate 1-base indices
    // the rest extra_bucket information is initialized in mehcached_table_reset()

    mehcached_table_reset(self);
#ifdef NDEBUG
    printf("NDEBUG\n");
#else
    printf("!NDEBUG (low performance)\n");
#endif

#ifdef MEHCACHED_VERBOSE
    printf("MEHCACHED_VERBOSE (low performance)\n");
#endif

#ifdef MEHCACHED_CONCURRENT
    printf("MEHCACHED_CONCURRENT (low performance)\n");
#endif

    printf("num_buckets = %u\n", self->num_buckets);
    printf("num_extra_buckets = %u\n", self->num_extra_buckets);
    printf("\n");
}

void mehcached_table_free(struct mehcached_table *self) {
    assert(self);
    kv_free(self->buckets);
}