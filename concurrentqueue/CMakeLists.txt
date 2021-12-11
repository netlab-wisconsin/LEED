cmake_minimum_required(VERSION 2.8.12)
project(moodycamel-concurrentqueue)

set(CMAKE_CXX_STANDARD 11)
add_library (concurrentqueue SHARED concurrentqueue.cpp)
set_target_properties(concurrentqueue PROPERTIES PUBLIC_HEADER concurrentqueue.h)
install(
        TARGETS concurrentqueue
        PUBLIC_HEADER          
)