include /home/pi/private/rocksdb-6.15.4/make_config.mk

#ifndef DISABLE_JEMALLOC
#	ifdef JEMALLOC
#		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
#	endif
#	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
#	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
#endif

#ifneq ($(USE_RTTI), 1)
#	CXXFLAGS += -fno-rtti
#endif

#.PHONY: clean librocksdb

all: rFAWN

#.c.o:
#	$(CC) $(CFLAGS) -c $< -o $@ -I/home/pi/private/rocksdb-6.15.4/include

#rFAWN: librocksdb rFAWN.o
#	$(CXX) $@.o -o$@ /home/pi/private/rocksdb-6.15.4/librocksdb.a $(PLATFORM_LDFLAGS) $(EXEC_LDFLAGS)

rFAWN: librocksdb rFAWN.o
	$(CXX) $@.o -o$@ /home/pi/private/rocksdb-6.15.4/librocksdb.a $(PLATFORM_LDFLAGS) $(EXEC_LDFLAGS)

librocksdb:
	cd /home/pi/private/rocksdb-6.15.4 && $(MAKE) static_lib

clean:
	rm -rf ./rFAWN