SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk
include $(SPDK_ROOT_DIR)/mk/spdk.modules.mk

APP = test_kv_table
SYS_LIBS += -lm -lstdc++
CXX_SRCS := ../utils/concurrentqueue.cpp
C_SRCS := ../kv_app.c ../kv_storage.c ../kv_log.c ../kv_table.c ../kv_table_concurrent.c kv_table_test.c

SPDK_LIB_LIST = $(ALL_MODULES_LIST)
SPDK_LIB_LIST += $(EVENT_BDEV_SUBSYSTEM)
SPDK_LIB_LIST += $(KV_BDEV_MODULES)

include $(SPDK_ROOT_DIR)/mk/spdk.app.mk
