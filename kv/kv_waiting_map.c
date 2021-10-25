#include "kv_waiting_map.h"

#include <assert.h>
#include <search.h>
#include <sys/queue.h>

#include "memery_operation.h"

struct kv_waiting_list_entry {
    struct kv_waiting_task task;
    STAILQ_ENTRY(kv_waiting_list_entry) next;
};
struct kv_waiting_entry {
    uint32_t index;
    STAILQ_HEAD(, kv_waiting_list_entry) tasks;
};

#define GET_INDEX(a) (((const struct kv_waiting_entry *)(a))->index)
static int compare(const void *pa, const void *pb) {
    if (GET_INDEX(pa) > GET_INDEX(pb))
        return 1;
    else if (GET_INDEX(pa) < GET_INDEX(pb))
        return -1;
    else
        return 0;
}
void kv_waiting_map_put(void **map, uint32_t index, kv_task_cb cb, void *cb_arg) {
    struct kv_waiting_entry *entry = kv_malloc(sizeof(struct kv_waiting_entry));
    entry->index = index;
    struct kv_waiting_entry **p = tsearch(entry, map, compare);
    assert(p);
    struct kv_waiting_list_entry *list_entry = kv_malloc(sizeof(struct kv_waiting_list_entry));
    list_entry->task = (struct kv_waiting_task){cb, cb_arg};
    if (*p == entry)
        STAILQ_INIT(&(*p)->tasks);
    else
        kv_free(entry);
    STAILQ_INSERT_TAIL(&(*p)->tasks, list_entry, next);
}
struct kv_waiting_task kv_waiting_map_get(void **map, uint32_t index) {
    struct kv_waiting_entry entry = {.index = index};
    struct kv_waiting_entry **p = tfind(&entry, map, compare);
    if (!p) return (struct kv_waiting_task){NULL, NULL};
    struct kv_waiting_list_entry *list_entry = STAILQ_FIRST(&(*p)->tasks);
    STAILQ_REMOVE_HEAD(&(*p)->tasks, next);
    if (STAILQ_EMPTY(&(*p)->tasks)) {
        struct kv_waiting_entry *tmp = *p;
        tdelete(*p, map, compare);
        kv_free(tmp);
    }
    struct kv_waiting_task ret = list_entry->task;
    kv_free(list_entry);
    return ret;
}