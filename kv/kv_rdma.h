#ifndef _KV_RDMA_H_
#define _KV_RDMA_H_
void * kv_rdma_alloc(void);
void kv_rdma_free(void *);
int kv_rdma_listen(void *self, char *addr_str, char *port_str);
#endif