#ifndef _KV_RING_H_
#define _KV_RING_H_
#include "kv_rdma.h"
void kv_ring_dispatch(char *key, connection_handle *h, uint32_t *ssd_id);
void kv_ring_init(char *etcd_ip, char *etcd_port, uint32_t thread_num);
void kv_ring_server_init(char *local_ip, char *local_port, uint32_t vid_num, uint32_t vid_per_ssd, uint32_t ssd_num);
void kv_ring_fini(void);
#endif