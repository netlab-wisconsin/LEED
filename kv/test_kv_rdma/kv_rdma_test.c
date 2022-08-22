#include "../kv_rdma.h"

#include <stdio.h>

#include "../kv_app.h"
static void handler(void *req_h, kv_rdma_mr req, uint32_t req_sz, void *arg) {
    // puts(buf);
    // sprintf(buf, "msg from server.");
    kv_rdma_make_resp(req_h, kv_rdma_get_req_buf(req), 1024);
}

static void rdma_start(void *arg) {
    kv_rdma_handle rdma;
    kv_rdma_init(&rdma, 1);
    kv_rdma_listen(rdma, "0.0.0.0", "9000", 32, 8192, handler, NULL, NULL, NULL);
}
int main(int argc, char **argv) { kv_app_start_single_task(argv[1], rdma_start, NULL); }