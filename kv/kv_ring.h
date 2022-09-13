#ifndef _KV_RING_H_
#define _KV_RING_H_
#include "kv_rdma.h"
enum { KV_RING_VNODE,
       KV_RING_TAIL,
       KV_RING_COPY };

typedef void (*kv_ring_cb)(void *arg);
typedef void (*kv_ring_req_handler)(void *req_h, kv_rdma_mr req, uint32_t req_sz, uint32_t ds_id,
                                    void *next, uint32_t vnode_type, void *arg);
typedef void (*kv_ring_copy_cb)(bool start, uint32_t ds_id, uint8_t *key_start, uint8_t *key_end, bool del, void *arg);

void kv_ring_dispatch(kv_rdma_mr req, kv_rdma_mr resp, void *resp_addr, kv_ring_cb cb, void *cb_arg);  // for clients
void kv_ring_forward(void *_node, kv_rdma_mr req, bool is_copy_req, kv_ring_cb cb, void *cb_arg);      // for servers

void kv_ring_register_copy_cb(kv_ring_copy_cb copy_cb, void *cb_arg);
void kv_ring_stop_copy(uint8_t *key_start, uint8_t *key_end);
// client: kv_ring_init(kv_rdma_init)
// server: kv_ring_init(kv_rdma_init)->kv_ring_server_init(kv_rdma_listen)
kv_rdma_handle kv_ring_init(char *etcd_ip, char *etcd_port, uint32_t thread_num, kv_ring_cb server_online_cb, void *arg);
void kv_ring_server_init(char *local_ip, char *local_port, uint32_t ring_num, uint32_t vid_per_ssd, uint32_t ds_num,
                         uint32_t rpl_num, uint32_t log_bkt_num, uint32_t con_req_num, uint32_t max_msg_sz, kv_ring_req_handler handler, void *arg,
                         kv_rdma_server_init_cb cb, void *cb_arg);
void kv_ring_fini(kv_rdma_fini_cb cb, void *cb_arg);
#endif