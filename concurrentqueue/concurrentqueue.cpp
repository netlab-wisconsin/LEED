#include "concurrentqueue.h"

#include "concurrentqueue.hpp"

typedef moodycamel::ConcurrentQueue<void*> MoodycamelCQType, *MoodycamelCQPtr;

extern "C" {

int moodycamel_cq_create(MoodycamelCQHandle* handle) {
    MoodycamelCQPtr retval = new MoodycamelCQType();
    if (retval == nullptr) {
        return 0;
    }
    *handle = retval;
    return 1;
}

int moodycamel_cq_destroy(MoodycamelCQHandle handle) {
    delete reinterpret_cast<MoodycamelCQPtr>(handle);
    return 1;
}

int moodycamel_cq_enqueue(MoodycamelCQHandle handle, MoodycamelValue value) {
    return reinterpret_cast<MoodycamelCQPtr>(handle)->enqueue(value) ? 1 : 0;
}

int moodycamel_cq_try_dequeue(MoodycamelCQHandle handle, MoodycamelValue* value) {
    return reinterpret_cast<MoodycamelCQPtr>(handle)->try_dequeue(*value) ? 1 : 0;
}

size_t moodycamel_cq_try_dequeue_bulk(MoodycamelCQHandle handle, MoodycamelValue* values, size_t max) {
    return reinterpret_cast<MoodycamelCQPtr>(handle)->try_dequeue_bulk(values, max);
}

size_t moodycamel_cq_size_approx(MoodycamelCQHandle handle) { return reinterpret_cast<MoodycamelCQPtr>(handle)->size_approx(); }
}
