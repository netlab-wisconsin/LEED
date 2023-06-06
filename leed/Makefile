
SPDK_ROOT_DIR := $(abspath $(CURDIR)/../..)
include $(SPDK_ROOT_DIR)/mk/spdk.common.mk

DIRS-y = app
DIRS-y += tests

.PHONY: all clean $(DIRS-y)

all: $(DIRS-y)
	@:

clean: $(DIRS-y)
	@:

include $(SPDK_ROOT_DIR)/mk/spdk.subdirs.mk
