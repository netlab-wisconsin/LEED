#ifndef _KV_RING_H_
#define _KV_RING_H_
#include "kv_rdma.h"
typedef void (*kv_ring_cb)(void *arg);
void kv_ring_dispatch(char *key, connection_handle *h, uint16_t *ssd_id);
void kv_ring_forward(char *key, uint32_t hop, uint32_t r_num, connection_handle *h, uint16_t *ssd_id);
// client: kv_ring_init(kv_rdma_init)
// server: kv_ring_init(kv_rdma_init)->kv_ring_server_init(kv_rdma_listen)
kv_rdma_handle kv_ring_init(char *etcd_ip, char *etcd_port, uint32_t thread_num, kv_ring_cb ready_cb, void *arg);
void kv_ring_server_init(char *local_ip, char *local_port, uint32_t vid_num, uint32_t vid_per_ssd, uint32_t ssd_num,
                         uint32_t con_req_num, uint32_t max_msg_sz, kv_rdma_req_handler handler, void *arg);
void kv_ring_fini(kv_rdma_fini_cb cb, void *cb_arg);
#endif