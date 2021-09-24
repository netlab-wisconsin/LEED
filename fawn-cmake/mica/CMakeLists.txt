
add_library (mica shm.c table.cpp util.c)
target_link_libraries (mica LINK_PUBLIC fawnds_flash)

target_include_directories (mica PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})