cmake_minimum_required(VERSION 2.8.12)
project(fawn_cmake)

set(CMAKE_CXX_STANDARD 11)
add_definitions(-D_FILE_OFFSET_BITS=64)

add_subdirectory (utils)
add_subdirectory (fawnds)


