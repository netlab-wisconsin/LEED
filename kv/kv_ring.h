#ifndef _KV_RING_H_
#define _KV_RING_H_
#include "kv_rdma.h"

typedef void (*kv_ring_cb)(void *arg);
typedef void (*kv_ring_req_handler)(void *req_h, kv_rdma_mr req, uint32_t req_sz, uint32_t ds_id,
                                    void *next, bool after_tail, void *arg);

void kv_ring_dispatch(kv_rdma_mr req, kv_rdma_mr resp, void *resp_addr, kv_ring_cb cb, void *cb_arg);  // for clients
void kv_ring_forward(void *node, kv_rdma_mr req, kv_ring_cb cb, void *cb_arg);                         // for servers

// client: kv_ring_init(kv_rdma_init)
// server: kv_ring_init(kv_rdma_init)->kv_ring_server_init(kv_rdma_listen)
kv_rdma_handle kv_ring_init(char *etcd_ip, char *etcd_port, uint32_t thread_num, kv_ring_cb server_online_cb, void *arg);
void kv_ring_server_init(char *local_ip, char *local_port, uint32_t ring_num, uint32_t vid_per_ssd, uint32_t ds_num,
                         uint32_t rpl_num, uint32_t log_bkt_num, uint32_t con_req_num, uint32_t max_msg_sz, kv_ring_req_handler handler, void *arg,
                         kv_rdma_server_init_cb cb, void *cb_arg);
void kv_ring_fini(kv_rdma_fini_cb cb, void *cb_arg);
#endif