
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

APP = kv_ring_ycsb_client

SYS_LIBS += -lm -lstdc++ -libverbs -lrdmacm -lkv_etcd

CXX_SRCS := ../utils/concurrentqueue.cpp ../ycsb/kv_ycsb.cpp ../ycsb/core/core_workload.cpp
C_SRCS := ../kv_memory.c ../kv_rdma.c ../kv_ds_queue.c ../kv_ring.c ../kv_app.c  ../utils/city.c ../utils/timing.c kv_client.c

SPDK_LIB_LIST = $(ALL_MODULES_LIST)
SPDK_LIB_LIST += $(EVENT_BDEV_SUBSYSTEM)
SPDK_LIB_LIST += $(KV_BDEV_MODULES)

include $(SPDK_ROOT_DIR)/mk/spdk.app.mk
