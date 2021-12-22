#ifndef _KV_MSG_H_
#define _KV_MSG_H_
#include <stdint.h>
struct kv_msg {
    uint8_t type;
    uint8_t key_len;
    uint32_t value_len;
    uint8_t data[0];

#define KV_MSG_OK 0
#define KV_MSG_GET 1
#define KV_MSG_SET 2
#define KV_MSG_DEL 3
#define KV_MSG_TEST 128
#define KV_MSG_ERR 255

#define KV_MSG_KEY(msg) ((msg)->data)
#define KV_MSG_VALUE(msg) ((msg)->data + (msg)->key_len)
#define KV_MSG_SIZE(msg) (sizeof(struct kv_msg) + (msg)->key_len + (msg)->value_len)
} __attribute__((packed));
#endif