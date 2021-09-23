# Add executable called "helloDemo" that is built from the source files
# "demo.cxx" and "demo_b.cxx". The extensions are automatically found.
add_executable (fawnds fawnds.cc hash_functions.cc fawnds_flash.cc fawnds_bench.cc)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

target_link_libraries (fawnds LINK_PUBLIC utils)
target_link_libraries(fawnds PRIVATE Threads::Threads OpenSSL::SSL)
