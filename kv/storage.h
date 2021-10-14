#ifndef _STORAGE_H_
#define _STORAGE_H_
#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
struct storage {
    uint32_t block_size;
    void *private_data;
};

typedef void (*storage_io_completion_cb)(bool success, void *cb_arg);

int storage_init(struct storage *self, const char *spdk_json_config_file);
int storage_fini(struct storage *self);
void storage_read(struct storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, storage_io_completion_cb cb, void *cb_arg);
void storage_write(struct storage *self, void *buf, int iovcnt, uint64_t offset, uint64_t nbytes, storage_io_completion_cb cb, void *cb_arg);

#endif