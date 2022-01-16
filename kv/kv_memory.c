#include "kv_memory.h"

#include <rte_mempool.h>

#include "spdk/env.h"
void *kv_dma_malloc(size_t size) { return spdk_dma_malloc(size, 4, NULL); }
void *kv_dma_zmalloc(size_t size) { return spdk_dma_zmalloc(size, 4, NULL); }
void kv_dma_free(void *buf) { spdk_dma_free(buf); }

struct kv_mempool *kv_mempool_create(const char *name, size_t count, size_t ele_size) {
    return (struct kv_mempool *)spdk_mempool_create(name, count, ele_size, SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_SOCKET_ID_ANY);
}

void kv_mempool_put(struct kv_mempool *mp, void *ele) { spdk_mempool_put((struct spdk_mempool *)mp, ele); }
void *kv_mempool_get(struct kv_mempool *mp) { return spdk_mempool_get((struct spdk_mempool *)mp); }
void kv_mempool_free(struct kv_mempool *mp) { spdk_mempool_free((struct spdk_mempool *)mp); }
uint64_t kv_mempool_get_id(struct kv_mempool *mp, void *ele) { return ele - ((struct rte_mempool *)mp)->mz->addr; }
void *kv_mempool_get_ele(struct kv_mempool *mp, uint64_t id) { return ((struct rte_mempool *)mp)->mz->addr + id; }