#include "../../kv_app.h"
#define TASK_NUM 8
#define MAX_MSG_NUM TASK_NUM * 10000000
uint64_t msg_num[TASK_NUM] = {0};
static void msg_sender(void *arg) {
    uint64_t *self = arg;
    if (++(*self) > MAX_MSG_NUM) {
        kv_app_stop(0);
    } else {
        uint64_t next = (self - msg_num + 1) % TASK_NUM;
        kv_app_send(next, msg_sender, msg_num + next);
    }
}

int main(int argc, char **argv) {
    struct kv_app_task task[TASK_NUM];
    for (size_t i = 0; i < TASK_NUM; i++) {
        task[i].func = msg_sender;
        task[i].arg = msg_num + i;
    }
    // task[0].func = msg_sender;
    kv_app_start(argv[1], TASK_NUM, task);
    return 0;
}
