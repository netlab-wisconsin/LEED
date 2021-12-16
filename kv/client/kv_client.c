#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "../kv_app.h"
#include "../kv_rdma.h"

static void on_req_fini(connection_handle h, bool success, kv_rmda_mr req, kv_rmda_mr resp, void *cb_arg) {
    assert(success);
    puts(kv_rdma_get_resp_buf(resp));
    kv_rdma_free_mr(req);
    kv_rdma_free_mr(resp);
    kv_rdma_disconnect(h);
}
static void connect_cb(connection_handle h, void *cb_arg) {
    if (!h) exit(-1);
    kv_rmda_mr req = kv_rdma_alloc_req(cb_arg, 1024);
    kv_rmda_mr resp = kv_rdma_alloc_resp(cb_arg, 1024);
    uint8_t *req_buf = kv_rdma_get_req_buf(req);
    sprintf(req_buf, "client request\n");
    kv_rmda_send_req(h, req, 32, resp, 1024, on_req_fini, NULL);
}
static void rdma_start(void *arg) {
    kv_rdma_handle rdma = kv_rdma_alloc();
    kv_rdma_connect(rdma, "192.168.1.13", "9000", connect_cb, rdma);
}
int main(int argc, char **argv) { kv_app_start_single_task(argv[1], rdma_start, NULL); }