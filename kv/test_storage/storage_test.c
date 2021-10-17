#include "../memery_operation.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "../kv_storage.h"

//----------- TEST --------------

char *buf;
static void read_cb(bool success, void *cb_arg)
{
    if (success)
    {
        SPDK_NOTICELOG("Read string from bdev : %s\n", buf);
    }
    else
    {
        SPDK_ERRLOG("bdev io read error\n");
    }
    kv_storage_stop(cb_arg);
}
struct iovec *iov;
static void write_cb(bool success, void *cb_arg)
{
    struct kv_storage *storage = cb_arg;
    kv_free(iov);
    kv_memset(buf, 0, 64);
    if (success)
    {
        SPDK_NOTICELOG("bdev io write completed successfully\n");
        kv_storage_read(storage, buf, 0, 0, 2 * storage->block_size, read_cb, storage);
    }
    else
    {
        SPDK_ERRLOG("bdev io write error: %d\n", EIO);
        kv_storage_stop(cb_arg);
    }
}

static void storage_start(void *arg)
{
    struct kv_storage *storage = arg;
    buf = kv_storage_malloc(storage, 2048);
    sprintf(buf, "hello world!\n");
    iov = kv_calloc(2, sizeof(struct iovec));
    iov[0].iov_base = buf;
    iov[0].iov_len = storage->block_size;
    iov[1].iov_base = buf + storage->block_size;
    iov[1].iov_len = storage->block_size;
    kv_storage_write(storage, iov, 2, 0, 2 * storage->block_size, write_cb, storage);
}

int main(int argc, char **argv)
{
    struct kv_storage storage;
    kv_storage_start(&storage, argv[1], storage_start, &storage);
    kv_storage_free(buf);
    return 0;
}
