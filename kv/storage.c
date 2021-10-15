#include "storage.h"

#include "memery_operation.h"
#include "spdk/bdev.h"
#include "spdk/bdev_module.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/stdinc.h"
#include "spdk/string.h"
#include "spdk/thread.h"

struct storage_private {
    pthread_t thread_id;
    pthread_mutex_t init_mtx;
    struct spdk_thread *app_thread;
    struct spdk_bdev *bdev;
    struct spdk_bdev_desc *bdev_desc;
    struct spdk_io_channel *bdev_io_channel;
};

#define priv(self) ((struct storage_private *)((self)->private_data))

struct storage_thread_arg {
    struct spdk_app_opts opts;
    struct storage *self;
};

struct storage_io_complete_arg {
    storage_io_completion_cb cb;
    void *cb_arg;
};

struct storage_io_wait_arg {
    struct storage *self;
    void *buf;
    int iovcnt;
    uint64_t offset;
    uint64_t nbytes;
    storage_io_completion_cb cb;
    void *cb_arg;
    bool is_read;
    struct spdk_bdev_io_wait_entry bdev_io_wait;
};

#define storage_io_wait_arg_init(arg, self, buf, iovcnt, offset, nbytes, cb, cb_arg, is_read) \
    do {                                                                                      \
        arg->self = self;                                                                     \
        arg->buf = buf;                                                                       \
        arg->iovcnt = iovcnt;                                                                 \
        arg->offset = offset;                                                                 \
        arg->nbytes = nbytes;                                                                 \
        arg->cb = cb;                                                                         \
        arg->cb_arg = cb_arg;                                                                 \
        arg->is_read = is_read;                                                               \
    } while (0)

static void
storage_io_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
    struct storage_io_complete_arg *arg = cb_arg;
    spdk_bdev_free_io(bdev_io);
    if (arg->cb)
        arg->cb(success, arg->cb_arg);
    kv_free(cb_arg);
}

static void storage_io_wait(void *_arg);

static void _storage_io(struct storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes,
                        storage_io_completion_cb cb, void *cb_arg, bool is_read, struct storage_io_wait_arg *wait_arg) {
    if (spdk_get_thread() != priv(self)->app_thread) {
        assert(!wait_arg);
        wait_arg = kv_malloc(sizeof(struct storage_io_wait_arg));
        storage_io_wait_arg_init(wait_arg, self, buf, iovcnt, offset, nbytes, cb, cb_arg, is_read);
        spdk_thread_send_msg(priv(self)->app_thread, storage_io_wait, wait_arg);
        return;
    }
    struct storage_io_complete_arg *arg = kv_malloc(sizeof(struct storage_io_complete_arg));
    arg->cb = cb;
    arg->cb_arg = cb_arg;
    int rc = 0;
    if (is_read) {
        if (iovcnt)
            rc = spdk_bdev_readv(priv(self)->bdev_desc, priv(self)->bdev_io_channel, buf, iovcnt, offset, nbytes, storage_io_complete, arg);
        else
            rc = spdk_bdev_read(priv(self)->bdev_desc, priv(self)->bdev_io_channel, buf, offset, nbytes, storage_io_complete, arg);
    } else {
        if (iovcnt)
            rc = spdk_bdev_writev(priv(self)->bdev_desc, priv(self)->bdev_io_channel, buf, iovcnt, offset, nbytes, storage_io_complete, arg);
        else
            rc = spdk_bdev_write(priv(self)->bdev_desc, priv(self)->bdev_io_channel, buf, offset, nbytes, storage_io_complete, arg);
    }
    if (rc == -ENOMEM) {
        SPDK_NOTICELOG("Queueing io\n");
        /* In case we cannot perform I/O now, queue I/O */
        if (!wait_arg) {
            wait_arg = kv_malloc(sizeof(struct storage_io_wait_arg));
            storage_io_wait_arg_init(wait_arg, self, buf, iovcnt, offset, nbytes, cb, cb_arg, is_read);
        }
        wait_arg->bdev_io_wait.bdev = priv(self)->bdev;
        wait_arg->bdev_io_wait.cb_fn = storage_io_wait;
        wait_arg->bdev_io_wait.cb_arg = arg;
        spdk_bdev_queue_io_wait(priv(self)->bdev, priv(self)->bdev_io_channel, &wait_arg->bdev_io_wait);
        return;
    }
    if (rc)
        storage_fini(self);
    if (wait_arg) {
        kv_free(wait_arg);
        wait_arg = NULL;
    }
}

static void storage_io_wait(void *_arg) {
    struct storage_io_wait_arg *arg = _arg;
    _storage_io(arg->self, arg->buf, arg->iovcnt, arg->offset, arg->nbytes, arg->cb, arg->cb_arg, arg->is_read, arg);
}

void storage_read(struct storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, storage_io_completion_cb cb, void *cb_arg) {
    _storage_io(self, buf, iovcnt, offset, nbytes, cb, cb_arg, true, NULL);
}

void storage_write(struct storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, storage_io_completion_cb cb, void *cb_arg) {
    _storage_io(self, buf, iovcnt, offset, nbytes, cb, cb_arg, false, NULL);
}

static void storage_create(void *arg) {
    struct storage *self = arg;
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
    priv(self)->bdev_io_channel = spdk_bdev_get_io_channel(priv(self)->bdev_desc);
    if (priv(self)->bdev_io_channel == NULL) {
        SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
        spdk_bdev_close(priv(self)->bdev_desc);
        spdk_app_stop(-1);
        return;
    }
    SPDK_NOTICELOG("Storage created.\n");
    priv(self)->app_thread = spdk_get_thread();
    self->block_size = spdk_bdev_get_block_size(priv(self)->bdev);
    pthread_mutex_unlock(&priv(self)->init_mtx);
}

static void *storage_thread(void *_arg) {
    struct storage_thread_arg *arg = _arg;
    int rc = 0;
    rc = spdk_app_start(&arg->opts, storage_create, arg->self);
    if (rc) {
        SPDK_ERRLOG("ERROR starting application\n");
        exit(rc);
    }
    spdk_app_fini();
    pthread_exit(arg);
}

int storage_init(struct storage *self, const char *spdk_json_config_file) {
    int rc = 0;
    struct storage_thread_arg *arg = kv_malloc(sizeof(struct storage_thread_arg));
    self->private_data = kv_malloc(sizeof(struct storage_private));
    pthread_mutex_init(&priv(self)->init_mtx, NULL);
    spdk_app_opts_init(&arg->opts);
    arg->opts.name = "storage";
    arg->opts.json_config_file = spdk_json_config_file;
    arg->self = self;
    pthread_mutex_lock(&priv(self)->init_mtx);
    if ((rc = pthread_create(&priv(self)->thread_id, NULL, storage_thread, arg)) != 0)
        SPDK_ERRLOG("Unable to spawn thread.\n");
    pthread_mutex_lock(&priv(self)->init_mtx); /* Wait for background thread to advance past the initialization */
    return rc;
}
static void storage_remove(void *arg) {
    struct storage *self = arg;
    SPDK_NOTICELOG("Stopping storage\n");
    spdk_put_io_channel(priv(self)->bdev_io_channel);
    spdk_bdev_close(priv(self)->bdev_desc);
    spdk_app_stop(0);
}

int storage_fini(struct storage *self) {
    void *arg;
    spdk_thread_send_msg(priv(self)->app_thread, storage_remove, self);
    pthread_join(priv(self)->thread_id, &arg);
    kv_free(arg);
    kv_free(self->private_data);
    return 0;
}
