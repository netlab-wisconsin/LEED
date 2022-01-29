#ifndef _KV_MSG_H_
#define _KV_MSG_H_
#include <stdint.h>

#include "kv_ds_queue.h"
struct kv_msg {
    uint8_t type;
    uint8_t key_len;
    uint16_t value_offset;
    uint32_t value_len;
    uint16_t ds_id;
    uint16_t hop;
    struct kv_ds_q_info q_info;
    uint8_t data[0];

#define KV_MSG_OK (0U)
#define KV_MSG_GET (1U)
#define KV_MSG_SET (2U)
#define KV_MSG_DEL (3U)
#define KV_MSG_TEST (128U)
#define KV_MSG_ERR (255U)

#define KV_MSG_KEY(msg) ((msg)->data)
#define KV_MSG_VALUE(msg) ((msg)->data + (msg)->value_offset)
#define KV_MSG_SIZE(msg) (sizeof(struct kv_msg) + (msg)->value_offset + (msg)->value_len)
} __attribute__((packed));
#define KV_MSG_MAX_HEADER_SIZE (sizeof(struct kv_msg) + 24)
#endif