
add_executable(mica util.c table.cpp bench.cpp)
target_link_libraries (mica PUBLIC fawnds_flash)

# target_include_directories (mica PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})