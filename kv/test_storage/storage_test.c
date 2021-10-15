#include "../memery_operation.h"
#include "pthread.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "../storage.h"

//----------- TEST --------------

static pthread_mutex_t g_io_mtx = PTHREAD_MUTEX_INITIALIZER;
static void read_cb(bool success, void *cb_arg) {
    if (success) {
        SPDK_NOTICELOG("Read string from bdev : %s\n", (char *)cb_arg);
    } else {
        SPDK_ERRLOG("bdev io read error\n");
    }
    pthread_mutex_unlock(&g_io_mtx);
}
static void write_cb(bool success, void *cb_arg) {
    if (success) {
        SPDK_NOTICELOG("bdev io write completed successfully\n");
    } else {
        SPDK_ERRLOG("bdev io write error: %d\n", EIO);
        exit(-1);
    }
    pthread_mutex_unlock(&g_io_mtx);
}

int main(int argc, char **argv) {
    char buf[512] = "world!";
    struct storage data;
    storage_init(&data, argv[1]);
    struct iovec *iov = kv_calloc(2, sizeof(struct iovec));
    iov[0].iov_base = "hello ";
    iov[0].iov_len = 6;
    iov[1].iov_base = buf;
    iov[1].iov_len = 512 - 6;
    pthread_mutex_lock(&g_io_mtx);
    storage_write(&data, iov, 2, 0, data.block_size, write_cb, NULL);
    pthread_mutex_lock(&g_io_mtx);
    kv_free(iov);
    storage_read(&data, buf, 0, 0, data.block_size, read_cb, buf);
    pthread_mutex_lock(&g_io_mtx);
    storage_fini(&data);
    return 0;
}
