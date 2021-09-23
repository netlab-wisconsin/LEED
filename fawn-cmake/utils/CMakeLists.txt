
add_library (utils hashutil.cc dbid.cc print.cc timing.c fawnnet.cc)

# Make sure the compiler can find include files for our Hello library
# when other libraries or executables link to Hello
target_include_directories (utils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})