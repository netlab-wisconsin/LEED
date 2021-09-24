
add_library (mica shm.c table.c util.c)


target_include_directories (mica PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})