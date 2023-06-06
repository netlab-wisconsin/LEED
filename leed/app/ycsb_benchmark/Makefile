
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

APP = kv_ycsb_benchmark
SYS_LIBS += -lm -lstdc++
CXX_SRCS := ../../utils/concurrentqueue.cpp ../../ycsb/kv_ycsb.cpp ../../ycsb/core/core_workload.cpp ../../kv_bucket.cpp
C_SRCS := ../../kv_memory.c ../../kv_app.c ../../kv_storage.c ../../kv_circular_log.c ../../kv_value_log.c ../../kv_bucket_log.c ../../kv_data_store.c ../../kv_ds_queue.c ../../utils/city.c ../../utils/timing.c benchmark.c

SPDK_LIB_LIST = $(ALL_MODULES_LIST)
SPDK_LIB_LIST += $(EVENT_BDEV_SUBSYSTEM)
SPDK_LIB_LIST += $(KV_BDEV_MODULES)

include $(SPDK_ROOT_DIR)/mk/spdk.app.mk
