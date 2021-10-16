#include "../memery_operation.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "../kv_storage.h"

//----------- TEST --------------

char buf[512] = "world!";
static void read_cb(bool success, void *cb_arg) {
    if (success) {
        SPDK_NOTICELOG("Read string from bdev : %s\n", buf);
    } else {
        SPDK_ERRLOG("bdev io read error\n");
    }
    kv_storage_stop(cb_arg);
}
struct iovec *iov;
static void write_cb(bool success, void *cb_arg) {
    struct kv_storage *storage=cb_arg;
    kv_free(iov);
    if (success) {
        SPDK_NOTICELOG("bdev io write completed successfully\n");
    } else {
        SPDK_ERRLOG("bdev io write error: %d\n", EIO);
        exit(-1);
    }
    kv_storage_read(storage, buf, 0, 0, storage->block_size, read_cb, storage);
}

static void storage_start(void * arg){
    struct kv_storage *storage=arg;
    iov = kv_calloc(2, sizeof(struct iovec));
    iov[0].iov_base = "hello ";
    iov[0].iov_len = 6;
    iov[1].iov_base = buf;
    iov[1].iov_len = 512 - 6;
    kv_storage_write(storage, iov, 2, 0, storage->block_size, write_cb, storage);
}

int main(int argc, char **argv) {
    struct kv_storage storage;
    kv_storage_init(&storage, argv[1],storage_start,&storage);
    return 0;
}
