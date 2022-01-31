
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

DIRS-y = test_storage
DIRS-y += test_kv_log
DIRS-y += test_kv_table
DIRS-y += benchmark
DIRS-y += test_kv_value_log
DIRS-y += test_kv_circular_log
DIRS-y += test_kv_data_store
DIRS-y += benchmark2
DIRS-y += benchmark3
DIRS-y += test_kv_rdma
DIRS-y += test_kv_app
DIRS-y += client
DIRS-y += server
DIRS-y += test_kv_ring
DIRS-y += ring_client
DIRS-y += ring_server
DIRS-y += ring_ycsb_client
DIRS-y += ycsb_benchmark


.PHONY: all clean $(DIRS-y)

all: $(DIRS-y)
	@:

clean: $(DIRS-y)
	@:

include $(SPDK_ROOT_DIR)/mk/spdk.subdirs.mk
