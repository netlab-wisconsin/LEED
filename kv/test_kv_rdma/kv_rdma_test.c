#include "../kv_rdma.h"

#include "../kv_app.h"
static void rdma_start(void *arg) {
    void *rdma = kv_rdma_alloc();
    kv_rdma_listen(rdma, "0.0.0.0", "9000");
}
int main(int argc, char **argv) { kv_app_start_single_task(argv[1], rdma_start, NULL); }