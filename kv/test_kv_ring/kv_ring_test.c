
#include "../kv_ring.h"

#include <stdio.h>

#include "../kv_app.h"
static void handler(void *req_h, kv_rmda_mr req, uint32_t req_sz, void *arg) {
    uint8_t *buf = kv_rdma_get_req_buf(req);
    puts(buf);
    sprintf(buf, "msg from server.");
    kv_rdma_make_resp(req, buf, 1024);
}
static void ready_cb(void *arg) {
    kv_ring_server_init("192.168.1.20", "9000", 32, 8, 4, 1, 64, 1024, handler, NULL, NULL, NULL);
}
static void ring_start(void *arg) { kv_ring_init("127.0.0.1", "2379", 1, ready_cb, NULL); }
int main(int argc, char **argv) { kv_app_start_single_task(argv[1], ring_start, NULL); }