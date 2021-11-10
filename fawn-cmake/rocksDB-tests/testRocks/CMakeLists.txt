add_executable(rFAWN
	rFAWN.cc)
target_link_libraries(rFAWN
  ${ROCKSDB_LIB})

include_directories("/home/pi/private/rocksdb-6.15.4/include")
