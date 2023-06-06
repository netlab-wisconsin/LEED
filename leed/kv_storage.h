#ifndef _KV_STORAGE_H_
#define _KV_STORAGE_H_
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
struct kv_storage{
    uint32_t block_size;
    uint32_t align;
    uint64_t num_blocks;
    void *private_data;
};


typedef void (*kv_storage_io_cb)(bool success, void *cb_arg);


void *kv_storage_malloc(struct kv_storage *self, size_t size);
void *kv_storage_zmalloc(struct kv_storage *self, size_t size);
void *kv_storage_blk_alloc(struct kv_storage *self, uint64_t n);
void *kv_storage_zblk_alloc(struct kv_storage *self, uint64_t n);
void kv_storage_free(void *buf);


void kv_storage_read(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, kv_storage_io_cb cb,
                     void *cb_arg);
void kv_storage_write(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, kv_storage_io_cb cb,
                      void *cb_arg);
void kv_storage_read_blocks(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t n, kv_storage_io_cb cb,
                            void *cb_arg);
void kv_storage_write_blocks(struct kv_storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t n, kv_storage_io_cb cb,
                             void *cb_arg);


int kv_storage_init(struct kv_storage *self, uint32_t index);
void kv_storage_fini(struct kv_storage *self);
//void *kv_storage_get_channel(struct kv_storage *self);
#endif