#ifndef _KV_INDEX_LOG_H_
#define _KV_INDEX_LOG_H_
#include "kv_circular_log.h"
#define KV_INDEX_LOG_ENTRY_PER_BLOCK 128

struct kv_index_log {
    struct kv_circular_log log;
    uint32_t *append_buf[2], *read_buf[2];
    uint8_t append_buf_i, read_buf_i;
    uint32_t buf_len;
    bool is_prefetch_running, is_dump_running;
};
// offset
// block index|7 bit|8 bit|

void kv_index_log_init(struct kv_index_log *self, struct kv_storage *storage, uint64_t base, uint64_t size, uint64_t head,
                       uint64_t tail, uint32_t buf_len);

uint64_t kv_index_log_get_offset_base(struct kv_index_log *self);
uint32_t *kv_index_log_read(struct kv_index_log *self);  // return buf_len * KV_INDEX_LOG_ENTRY_PER_BLOCK entries

void kv_index_log_write(struct kv_index_log *self, uint64_t offset, uint32_t bucket_index);
void kv_index_log_fini(struct kv_index_log *self);
#endif