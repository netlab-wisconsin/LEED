#include "kv_storage.h"

#include "memery_operation.h"
#include "spdk/bdev.h"
#include "spdk/bdev_module.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/stdinc.h"
#include "spdk/string.h"
#include "spdk/thread.h"

struct private_data_t {
    struct spdk_bdev *bdev;
    struct spdk_bdev_desc *bdev_desc;
    struct spdk_io_channel *io_channel;
};
#define priv(self) ((struct private_data_t *)((self)->private_data))

struct io_complete_arg {
    kv_storage_io_cb cb;
    void *cb_arg;
};

struct io_wait_arg {
    struct kv_storage *self;
    void *buf;
    int iovcnt;
    uint64_t offset;
    uint64_t n;
    kv_storage_io_cb cb;
    void *cb_arg;
    bool is_read, is_block;
    struct spdk_bdev_io_wait_entry bdev_io_wait;
};

#define io_wait_arg_init(arg, dev, buf, iovcnt, offset, n, cb, cb_arg, is_read, is_block) \
    do {                                                                                  \
        arg->dev = dev;                                                                   \
        arg->buf = buf;                                                                   \
        arg->iovcnt = iovcnt;                                                             \
        arg->offset = offset;                                                             \
        arg->n = n;                                                                       \
        arg->cb = cb;                                                                     \
        arg->cb_arg = cb_arg;                                                             \
        arg->is_read = is_read;                                                           \
        arg->is_block = is_block;                                                         \
    } while (0)

static void io_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
    struct io_complete_arg *arg = cb_arg;
    spdk_bdev_free_io(bdev_io);
    if (arg->cb) arg->cb(success, arg->cb_arg);
    kv_free(cb_arg);
}

static void storage_io_wait(void *_arg);

static void _storage_io(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t n, kv_storage_io_cb cb,
                        void *cb_arg, bool is_read, bool is_block, struct io_wait_arg *wait_arg) {
    typedef int (*func_t)(struct spdk_bdev_desc *, struct spdk_io_channel *, void *, uint64_t, uint64_t,
                          spdk_bdev_io_completion_cb, void *);
    typedef int (*funcv_t)(struct spdk_bdev_desc *, struct spdk_io_channel *, struct iovec * iov, int, uint64_t, uint64_t,
                           spdk_bdev_io_completion_cb, void *);
    static const func_t func[] = {spdk_bdev_write, spdk_bdev_read, spdk_bdev_write_blocks, spdk_bdev_read_blocks};
    static const funcv_t funcv[] = {spdk_bdev_writev, spdk_bdev_readv, spdk_bdev_writev_blocks, spdk_bdev_readv_blocks};
    struct io_complete_arg *arg = kv_malloc(sizeof(struct io_complete_arg));
    arg->cb = cb;
    arg->cb_arg = cb_arg;
    int rc = 0;
    uint32_t i = ((is_block ? 1 : 0) << 1) | (is_read ? 1 : 0);
    if (iovcnt)
        funcv[i](priv(self)->bdev_desc, priv(self)->io_channel, buf, iovcnt, offset, n, io_complete, arg);
    else
        func[i](priv(self)->bdev_desc, priv(self)->io_channel, buf, offset, n, io_complete, arg);
    if (rc == -ENOMEM) {
        SPDK_NOTICELOG("Queueing io\n");
        /* In case we cannot perform I/O now, queue I/O */
        if (!wait_arg) {
            wait_arg = kv_malloc(sizeof(struct io_wait_arg));
            if (iovcnt) {
                struct iovec *iov = kv_calloc(iovcnt, sizeof(struct iovec));
                kv_memcpy(iov, buf, iovcnt * sizeof(struct iovec));
                buf = iov;
            }
            io_wait_arg_init(wait_arg, self, buf, iovcnt, offset, n, cb, cb_arg, is_read, is_block);
        }
        wait_arg->bdev_io_wait.bdev = priv(self)->bdev;
        wait_arg->bdev_io_wait.cb_fn = storage_io_wait;
        wait_arg->bdev_io_wait.cb_arg = arg;
        spdk_bdev_queue_io_wait(priv(self)->bdev, priv(self)->io_channel, &wait_arg->bdev_io_wait);
    } else if (wait_arg) {
        if (iovcnt) kv_free(buf);
        kv_free(wait_arg);
    }
}

static void storage_io_wait(void *_arg) {
    struct io_wait_arg *arg = _arg;
    _storage_io(arg->self, arg->buf, arg->iovcnt, arg->offset, arg->n, arg->cb, arg->cb_arg, arg->is_read, arg->is_block, arg);
}

void kv_storage_read(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, kv_storage_io_cb cb,
                     void *cb_arg) {
    _storage_io(self, buf, iovcnt, offset, nbytes, cb, cb_arg, true, false, NULL);
}

void kv_storage_write(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, kv_storage_io_cb cb,
                      void *cb_arg) {
    _storage_io(self, buf, iovcnt, offset, nbytes, cb, cb_arg, false, false, NULL);
}

void kv_storage_read_blocks(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t n, kv_storage_io_cb cb,
                            void *cb_arg) {
    _storage_io(self, buf, iovcnt, offset, n, cb, cb_arg, true, true, NULL);
}

void kv_storage_write_blocks(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t n, kv_storage_io_cb cb,
                             void *cb_arg) {
    _storage_io(self, buf, iovcnt, offset, n, cb, cb_arg, false, true, NULL);
}

int kv_storage_init(struct kv_storage *self, uint32_t index) {
    self->private_data = kv_malloc(sizeof(struct private_data_t));
    priv(self)->bdev = spdk_bdev_first();
    for (uint32_t i = 0; priv(self)->bdev; ++i) {
        if (i == index) break;
        priv(self)->bdev = spdk_bdev_next(priv(self)->bdev);
    }
    if (!priv(self)->bdev) {
        SPDK_ERRLOG("No avaliable bdev\n");
        kv_free(self->private_data);
        return -1;
    }
    int rc = 0;
    SPDK_NOTICELOG("Opening the bdev %s\n", priv(self)->bdev->name);
    rc = spdk_bdev_open(priv(self)->bdev, true, NULL, NULL, &priv(self)->bdev_desc);
    if (rc) {
        SPDK_ERRLOG("Could not open bdev: %s\n", priv(self)->bdev->name);
        kv_free(self->private_data);
        return rc;
    }
    priv(self)->io_channel = spdk_bdev_get_io_channel(priv(self)->bdev_desc);
    if (priv(self)->io_channel == NULL) {
        SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
        spdk_bdev_close(priv(self)->bdev_desc);
        kv_free(self->private_data);
        return rc;
    }
    self->block_size = spdk_bdev_get_block_size(priv(self)->bdev);
    self->align = spdk_bdev_get_buf_align(priv(self)->bdev);
    self->num_blocks = spdk_bdev_get_num_blocks(priv(self)->bdev);

    return rc;
}

void kv_storage_fini(struct kv_storage *self) {
    spdk_put_io_channel(priv(self)->io_channel);
    spdk_bdev_close(priv(self)->bdev_desc);
    kv_free(self->private_data);
}

void *kv_storage_malloc(struct kv_storage *self, size_t size) { return spdk_dma_malloc(size, self->align, NULL); }
void *kv_storage_zmalloc(struct kv_storage *self, size_t size) { return spdk_dma_zmalloc(size, self->align, NULL); }
void *kv_storage_blk_alloc(struct kv_storage *self, uint64_t n) {
    return spdk_dma_malloc(n * self->block_size, self->align, NULL);
}
void *kv_storage_zblk_alloc(struct kv_storage *self, uint64_t n) {
    return spdk_dma_zmalloc(n * self->block_size, self->align, NULL);
}
void kv_storage_free(void *buf) { spdk_free(buf); }