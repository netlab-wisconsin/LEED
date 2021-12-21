#include "../kv_rdma.h"

#include <stdio.h>

#include "../kv_app.h"
static void handler(void *req, uint8_t *buf, uint32_t req_sz, void *arg) {
    //puts(buf);
    //sprintf(buf, "msg from server.");
    kv_rdma_make_resp(req, buf, 1024);
}

static void connect_cb(connection_handle h, void *cb_arg) {
    puts("client connected.");
    kv_rdma_setup_conn_ctx(h, 32, 8192, handler, NULL);
}

static void rdma_start(void *arg) {
    kv_rdma_handle rdma;
    kv_rdma_init(&rdma);
    kv_rdma_listen(rdma, "0.0.0.0", "9000", connect_cb, NULL);
}
int main(int argc, char **argv) { kv_app_start_single_task(argv[1], rdma_start, NULL); }