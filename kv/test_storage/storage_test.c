#include "../kv_app.h"
#include "../kv_storage.h"
#include "../memery_operation.h"
#include "spdk/log.h"
#include "spdk/string.h"

//----------- TEST --------------
struct kv_storage storages[3];
char *buf[3];
struct iovec *iov[3];
static void storage_start(void *arg);
static void read_cb(bool success, void *cb_arg) {
    struct kv_storage *storage = cb_arg;
    uint32_t index = storage - storages;
    if (success) {
        SPDK_NOTICELOG("Read string from bdev : %s\n", buf[index]);
    } else {
        SPDK_ERRLOG("bdev io read error\n");
    }
    kv_storage_fini(storage);
    kv_app_stop(0);
    if (index == 1) kv_app_send_msg(kv_app().threads[2], storage_start, storages + 2);
}

static void write_cb(bool success, void *cb_arg) {
    struct kv_storage *storage = cb_arg;
    uint32_t index = storage - storages;
    kv_free(iov[index]);
    kv_memset(buf[index], 0, 64);
    if (success) {
        SPDK_NOTICELOG("bdev io write completed successfully\n");
        kv_storage_read(storage, buf[index], 0, 0, 2 * storage->block_size, read_cb, storage);
    } else {
        SPDK_ERRLOG("bdev io write error: %d\n", EIO);
        kv_storage_fini(storage);
        kv_app_stop(-1);
    }
}

static void storage_start(void *arg) {
    struct kv_storage *storage = arg;
    uint32_t index = storage - storages;
    if(kv_storage_init(storage, index&1)){
        kv_app_stop(-1);
        return;
    }
    buf[index] = kv_storage_malloc(storage, 2048);
    sprintf(buf[index], "%u. hello world!\n", index);
    iov[index] = kv_calloc(2, sizeof(struct iovec));
    iov[index][0].iov_base = buf[index];
    iov[index][0].iov_len = storage->block_size;
    iov[index][1].iov_base = buf[index] + storage->block_size;
    iov[index][1].iov_len = storage->block_size;
    kv_storage_write(storage, iov[index], 2, 0, 2 * storage->block_size, write_cb, storage);
}

int main(int argc, char **argv) {
    struct kv_app_task task[] = {{storage_start, storages}, {storage_start, storages + 1}, {NULL, NULL}};
    kv_app_start(argv[1], 3, task);
    return 0;
}
