#ifndef _KV_STORAGE_H_
#define _KV_STORAGE_H_
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
struct kv_storage {
    uint32_t block_size;
    uint32_t align;
    void *private_data;
};

typedef void (*kv_storage_io_cb)(bool success, void *cb_arg);
typedef void (*kv_storage_start_fn)(void *ctx);

void *kv_storage_malloc(struct kv_storage *self, size_t size);
void kv_storage_free(void *buf);
int kv_storage_start(struct kv_storage *self, const char *spdk_json_config_file, kv_storage_start_fn start_fn, void *fn_arg);
void kv_storage_stop(struct kv_storage *self);
void kv_storage_read(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, kv_storage_io_cb cb,
                     void *cb_arg);
void kv_storage_write(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, kv_storage_io_cb cb,
                      void *cb_arg);

#endif