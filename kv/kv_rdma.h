#ifndef _KV_RDMA_H_
#define _KV_RDMA_H_
#include <stdbool.h>
#include <stdint.h>
typedef void *connection_handle;
typedef void *kv_rdma_handle;
typedef void *kv_rmda_mr;

typedef void (*kv_rdma_req_cb)(connection_handle h, bool success, kv_rmda_mr req, kv_rmda_mr resp, void *cb_arg);
typedef void (*kv_rdma_connect_cb)(connection_handle h, void *cb_arg);
typedef void (*kv_rdma_disconnect_cb)(void *cb_arg);
typedef void (*kv_rdma_req_handler)(void *req, uint8_t *buf, uint32_t req_sz, void *arg);
typedef void (*kv_rdma_fini_cb)(void *ctx);

void kv_rdma_init(kv_rdma_handle *h, uint32_t thread_num);
void kv_rdma_fini(kv_rdma_handle h, kv_rdma_fini_cb cb, void *cb_arg);

kv_rmda_mr kv_rdma_alloc_req(kv_rdma_handle h, uint32_t size);
uint8_t *kv_rdma_get_req_buf(kv_rmda_mr mr);
kv_rmda_mr kv_rdma_alloc_resp(kv_rdma_handle h, uint32_t size);
uint8_t *kv_rdma_get_resp_buf(kv_rmda_mr mr);
void kv_rdma_free_mr(kv_rmda_mr h);

int kv_rdma_listen(kv_rdma_handle h, char *addr_str, char *port_str, uint32_t con_req_num, uint32_t max_msg_sz,
                   kv_rdma_req_handler handler, void *arg);
void kv_rdma_make_resp(void *req, uint8_t *resp, uint32_t resp_sz);  // resp must within buf

int kv_rdma_connect(kv_rdma_handle self, char *addr_str, char *port_str, kv_rdma_connect_cb cb, void *cb_arg);
void kv_rmda_send_req(connection_handle h, kv_rmda_mr req, uint32_t req_sz, kv_rmda_mr resp, kv_rdma_req_cb cb, void *cb_arg);
void kv_rdma_disconnect(connection_handle h, kv_rdma_disconnect_cb cb, void *cb_arg);
#endif