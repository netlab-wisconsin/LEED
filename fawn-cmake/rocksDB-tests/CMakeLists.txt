# Add executable called "helloDemo" that is built from the source files
# "demo.cxx" and "demo_b.cxx". The extensions are automatically found.
add_executable (fawnds fawnds.cc hash_functions.cc fawnds_flash.cc fawnds_bench.cc)
#add_executable (fawnds fawnds.cc)

#target_link_libraries(fawnds ${OCKSDB_LIB})

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

#target_link_libraries (fawnds LINK_PUBLIC utils)
#target_link_libraries(fawnds PRIVATE Threads::Threads OpenSSL::SSL)
target_link_libraries(fawnds Threads::Threads OpenSSL::SSL utils -lrt -lz -lsnappy zstd -lbz2 -ldl)

include_directories("/home/pi/private/rocksdb-6.15.4/include")
include_directories("/home/pi/private/rFAWN/fawn-cmake/")

add_subdirectory (/home/pi/private/rFAWN/fawn-cmake/utils utils)
#add_subdirectory( ${/home/pi/private/rFAWN/fawn-cmake} build)
add_library (fawnds_flash fawnds_flash.cc)
target_include_directories (fawnds_flash PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
