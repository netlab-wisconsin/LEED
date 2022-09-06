# Run FAWN-KV-YCSB in SmartNIC-JBOF or Pi

## Pre
1.  sudo apt install autoconf libtool libtbb-dev libgtest-dev libboost-all-dev libboost-thread-dev  pkg-config bison flex
2.  cd /usr/src/gtest/ && sudo cmake CMakeLists.txt  && sudo make && sudo cp *.a /usr/lib
3.  sudo ln -s /usr/lib/aarch64-linux-gnu/libboost_thread.so.1.58.0 /usr/lib/aarch64-linux-gnu/libboost_thread-mt.so

## Thrift
1.  git clone https://github.com/thunderZH963/Thrift-0.5.0-on-SmartNIC-Pi
2.  ./bootstrap.sh
3.  ./configure
4.  sudo ldconfig
5.  thrift -version to check if sucessfully
   
## Install
1.  autoreconf -is
2.  ./configure
3.  make
