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
    struct spdk_thread *app_thread;
    struct spdk_bdev *bdev;
    struct spdk_bdev_desc *bdev_desc;
    struct spdk_io_channel *io_channel;
    kv_storage_start_fn start_fn;
    void *fn_arg;
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
    uint64_t nbytes;
    kv_storage_io_cb cb;
    void *cb_arg;
    bool is_read;
    struct spdk_bdev_io_wait_entry bdev_io_wait;
};

#define io_wait_arg_init(arg, self, buf, iovcnt, offset, nbytes, cb, cb_arg, is_read) \
    do {                                                                              \
        arg->self = self;                                                             \
        arg->buf = buf;                                                               \
        arg->iovcnt = iovcnt;                                                         \
        arg->offset = offset;                                                         \
        arg->nbytes = nbytes;                                                         \
        arg->cb = cb;                                                                 \
        arg->cb_arg = cb_arg;                                                         \
        arg->is_read = is_read;                                                       \
    } while (0)

static void io_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
    struct io_complete_arg *arg = cb_arg;
    spdk_bdev_free_io(bdev_io);
    if (arg->cb) arg->cb(success, arg->cb_arg);
    kv_free(cb_arg);
}

static void storage_io_wait(void *_arg);

static void _storage_io(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, kv_storage_io_cb cb,
                        void *cb_arg, bool is_read, struct io_wait_arg *wait_arg) {
    //assert(spdk_get_thread() == priv(self)->app_thread);
    struct io_complete_arg *arg = kv_malloc(sizeof(struct io_complete_arg));
    arg->cb = cb;
    arg->cb_arg = cb_arg;
    int rc = 0;
    switch (((is_read ? 1 : 0) << 1) | (iovcnt ? 1 : 0)) {
        case 0:
            rc = spdk_bdev_write(priv(self)->bdev_desc, priv(self)->io_channel, buf, offset, nbytes, io_complete, arg);
            break;
        case 1:
            rc = spdk_bdev_writev(priv(self)->bdev_desc, priv(self)->io_channel, buf, iovcnt, offset, nbytes, io_complete, arg);
            break;
        case 2:
            rc = spdk_bdev_read(priv(self)->bdev_desc, priv(self)->io_channel, buf, offset, nbytes, io_complete, arg);
            break;
        case 3:
            rc = spdk_bdev_readv(priv(self)->bdev_desc, priv(self)->io_channel, buf, iovcnt, offset, nbytes, io_complete, arg);
    }
    if (rc == -ENOMEM) {
        SPDK_NOTICELOG("Queueing io\n");
        /* In case we cannot perform I/O now, queue I/O */
        if (!wait_arg) {
            wait_arg = kv_malloc(sizeof(struct io_wait_arg));
            io_wait_arg_init(wait_arg, self, buf, iovcnt, offset, nbytes, cb, cb_arg, is_read);
        }
        wait_arg->bdev_io_wait.bdev = priv(self)->bdev;
        wait_arg->bdev_io_wait.cb_fn = storage_io_wait;
        wait_arg->bdev_io_wait.cb_arg = arg;
        spdk_bdev_queue_io_wait(priv(self)->bdev, priv(self)->io_channel, &wait_arg->bdev_io_wait);

    } else if (wait_arg)
        kv_free(wait_arg);
}

static void storage_io_wait(void *_arg) {
    struct io_wait_arg *arg = _arg;
    _storage_io(arg->self, arg->buf, arg->iovcnt, arg->offset, arg->nbytes, arg->cb, arg->cb_arg, arg->is_read, arg);
}

void kv_storage_read(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, kv_storage_io_cb cb,
                  void *cb_arg) {
    _storage_io(self, buf, iovcnt, offset, nbytes, cb, cb_arg, true, NULL);
}

void kv_storage_write(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, kv_storage_io_cb cb,
                   void *cb_arg) {
    _storage_io(self, buf, iovcnt, offset, nbytes, cb, cb_arg, false, NULL);
}

static void kv_storage_create(void *arg) {
    struct kv_storage *self = arg;
    int rc;
    priv(self)->bdev = spdk_bdev_first();
    if (!priv(self)->bdev) {
        SPDK_ERRLOG("No avaliable bdev\n");
        spdk_app_stop(-1);
        return;
    }
    SPDK_NOTICELOG("Opening the bdev %s\n", priv(self)->bdev->name);
    rc = spdk_bdev_open(priv(self)->bdev, true, NULL, NULL, &priv(self)->bdev_desc);
    if (rc) {
        SPDK_ERRLOG("Could not open bdev: %s\n", priv(self)->bdev->name);
        spdk_app_stop(-1);
        return;
    }
    SPDK_NOTICELOG("Opening io channel\n");
    priv(self)->io_channel = spdk_bdev_get_io_channel(priv(self)->bdev_desc);
    if (priv(self)->io_channel == NULL) {
        SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
        spdk_bdev_close(priv(self)->bdev_desc);
        spdk_app_stop(-1);
        return;
    }
    SPDK_NOTICELOG("Storage created.\n");
    priv(self)->app_thread = spdk_get_thread();
    self->block_size = spdk_bdev_get_block_size(priv(self)->bdev);
    if (priv(self)->start_fn) priv(self)->start_fn(priv(self)->fn_arg);
}

int kv_storage_start(struct kv_storage *self, const char *spdk_json_config_file, kv_storage_start_fn start_fn, void *fn_arg) {
    self->private_data = kv_malloc(sizeof(struct private_data_t));
    struct spdk_app_opts opts;
    spdk_app_opts_init(&opts);
    opts.name = "kv_storage";
    opts.json_config_file = spdk_json_config_file;
    priv(self)->start_fn = start_fn;
    priv(self)->fn_arg = fn_arg;
    int rc = 0;
    if ((rc = spdk_app_start(&opts, kv_storage_create, self)))
        SPDK_ERRLOG("ERROR starting application\n");
    spdk_app_fini();
    kv_free(self->private_data);
    return rc;
}

void kv_storage_stop(struct kv_storage *self) {
    assert(spdk_get_thread() == priv(self)->app_thread);
    SPDK_NOTICELOG("Stopping storage\n");
    spdk_put_io_channel(priv(self)->io_channel);
    spdk_bdev_close(priv(self)->bdev_desc);
    spdk_app_stop(0);
}