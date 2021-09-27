# Add executable called "helloDemo" that is built from the source files
# "demo.cxx" and "demo_b.cxx". The extensions are automatically found.
add_executable (fawnds fawnds.cc hash_functions.cc fawnds_flash.cc fawnds_bench.cc)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)
find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h PATHS)
include_directories(${ROCKSDB_INCLUDE_DIR})
find_path(ROCKSDB_INCLUDE_DIR rocksdb/options.h PATHS)
include_directories(${ROCKSDB_INCLUDE_DIR})

find_library(ROCKSDB_LIB rocksdb)
message(STATUS "finding ${ROCKSDB_LIB}")
target_link_libraries (fawnds LINK_PUBLIC utils)
target_link_libraries(fawnds PRIVATE Threads::Threads OpenSSL::SSL ${ROCKSDB_LIB} ${LZ4_LIB} -lpthread -lz -lsnappy -lbz2 -ldl)

add_library (fawnds_flash fawnds_flash.cc)
target_include_directories (fawnds_flash PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
