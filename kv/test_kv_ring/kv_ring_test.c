
#include "../kv_ring.h"

#include <stdio.h>

#include "../kv_app.h"

static void ring_start(void *arg) {
    kv_ring_init("127.0.0.1", "2379", 1);
    kv_ring_server_init("192.168.1.20", "9000", 32, 8, 4);
}
int main(int argc, char **argv) { kv_app_start_single_task(argv[1], ring_start, NULL); }