SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

APP = kv_server

SYS_LIBS += -lm -lstdc++ -libverbs -lrdmacm
CXX_SRCS := ../utils/concurrentqueue.cpp
C_SRCS := ../kv_app.c ../kv_storage.c ../kv_circular_log.c ../kv_value_log.c ../kv_bucket_log.c ../kv_data_store.c ../kv_memory.c ../kv_rdma.c  ../utils/city.c ../utils/timing.c kv_server.c

SPDK_LIB_LIST = $(ALL_MODULES_LIST)
SPDK_LIB_LIST += $(EVENT_BDEV_SUBSYSTEM)
SPDK_LIB_LIST += $(KV_BDEV_MODULES)

include $(SPDK_ROOT_DIR)/mk/spdk.app.mk
