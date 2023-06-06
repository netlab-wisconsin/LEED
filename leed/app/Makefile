
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

DIRS-y += ring_server
DIRS-y += ring_ycsb_client
DIRS-y += ycsb_benchmark

.PHONY: all clean $(DIRS-y)

all: $(DIRS-y)
	@:

clean: $(DIRS-y)
	@:

include $(SPDK_ROOT_DIR)/mk/spdk.subdirs.mk
