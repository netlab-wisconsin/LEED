
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

DIRS-y = kv_storage
DIRS-y += kv_value_log
DIRS-y += kv_circular_log
DIRS-y += kv_data_store
DIRS-y += kv_rdma
DIRS-y += kv_app
DIRS-y += kv_client
DIRS-y += kv_server


.PHONY: all clean $(DIRS-y)

all: $(DIRS-y)
	@:

clean: $(DIRS-y)
	@:

include $(SPDK_ROOT_DIR)/mk/spdk.subdirs.mk
