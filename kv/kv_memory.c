#include "kv_memory.h"

#include "spdk/env.h"
void *kv_dma_malloc(size_t size) { return spdk_dma_malloc(size, 4, NULL); }
void *kv_dma_zmalloc(size_t size) { return spdk_dma_zmalloc(size, 4, NULL); }
void kv_dma_free(void *buf) { spdk_dma_free(buf); }

struct _kv_mempool {
    struct spdk_mempool *mp;
    void *base_addr;
};
struct kv_mempool *kv_mempool_create(const char *name, size_t count, size_t ele_size) {
    struct spdk_mempool *spdk_mp =
        spdk_mempool_create(name, count, ele_size, SPDK_MEMPOOL_DEFAULT_CACHE_SIZE, SPDK_ENV_SOCKET_ID_ANY);
    if (spdk_mp == NULL) return NULL;
    struct _kv_mempool *_mp = kv_malloc(sizeof(struct _kv_mempool));
    _mp->mp = spdk_mp;
    _mp->base_addr = spdk_mempool_get(spdk_mp);
    spdk_mempool_put(spdk_mp, _mp->base_addr);
    return (struct kv_mempool *)_mp;
}

void kv_mempool_put(struct kv_mempool *mp, void *ele) { spdk_mempool_put(((struct _kv_mempool *)mp)->mp, ele); }
void *kv_mempool_get(struct kv_mempool *mp) { return spdk_mempool_get(((struct _kv_mempool *)mp)->mp); }
void kv_mempool_free(struct kv_mempool *mp) {
    spdk_mempool_free(((struct _kv_mempool *)mp)->mp);
    kv_free(mp);
}
int64_t kv_mempool_get_id(struct kv_mempool *mp, void *ele) { return ele - ((struct _kv_mempool *)mp)->base_addr; }
void *kv_mempool_get_ele(struct kv_mempool *mp, int64_t id) { return ((struct _kv_mempool *)mp)->base_addr + id; }