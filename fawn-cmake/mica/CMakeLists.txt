# add_definitions(-DMEHCACHED_CONCURRENT)
add_executable(mica util.c city.c table.cpp bench.cpp)
target_link_libraries (mica fawnds_flash utils)

# target_include_directories (mica PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})