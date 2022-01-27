#ifndef _KV_YCSB_H_
#define _KV_YCSB_H_
#ifdef __cplusplus
extern "C" {
#endif
#include <stdint.h>
enum kv_ycsb_operation { YCSB_INSERT, YCSB_READ, YCSB_UPDATE, YCSB_SCAN, YCSB_READMODIFYWRITE, YCSB_SEQ };
typedef void *kv_ycsb_handle;
void kv_ycsb_init(kv_ycsb_handle *self, char *propertyfile, uint64_t *record_cnt, uint64_t *operation_cnt,
                  uint32_t *value_size);
void kv_ycsb_fini(kv_ycsb_handle self);

enum kv_ycsb_operation kv_ycsb_next(kv_ycsb_handle self, bool is_seq, uint8_t *key, uint8_t *value);

#ifdef __cplusplus
}
#endif
#endif