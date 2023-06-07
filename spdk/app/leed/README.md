# LEED
LEED is developed on SPDK v19.10.1 and Broadcom Stingray SmartNIC platform. However, it is completely independent of the platform and supports x86 and etc. You can follow the following instructions to build and run LEED.

## Prerequisites

### spdk
Follow the [official guide](/spdk/README.md) to setup spdk with **rdma** support.

### go
Install [golang](https://go.dev/doc/install).

### ectd
Install [etcd v3.5](https://etcd.io/docs/v3.5/install/).

## Build

### kv_etcd
kv_etcd is a Go library that enables LEED to communicate with etcd.

```[bash]
cd src/kv_etcd
go get .
bash build_and_install.sh
```

### spdk

```[bash]
cd spdk
./configure --with-rdma
make
```

## Benchmarks
All the benchmarks can be found in the `src/app` directory.

### Datastore Benchmark
This benchmark can test the performance of LEED's datastore.

```[bash]
# ycsb_benchmark/kv_ycsb_benchmark -h

Program options:
  -h               Display this help message
  -c <config_file> Set the SPDK JSON config file: config.json
  -w <workload_file> Set the YCSB workload file: workloada.spec
  -d <ssd_num>     Set the number of SSDs: 2
  -P <producer_num> Set the number of YCSB threads : 1
  -i <io_num>      Set the number of concurrent I/Os: 32
  -R               Perform sequential read operations
  -W               Perform sequential write operations
  -D               Perform delete operations
  -F               Perform fill operations
```
### System Benchmark
Make sure to set up a working etcd cluster on the servers. ([guide](https://etcd.io/docs/v3.5/tutorials/how-to-setup-cluster/))

#### Server
LEED servers are designed to run on SmartNIC cluster.

```[bash]
# ring_server/kv_ring_server -h

Program options:
  -h               Display this help message
  -n <num_items>   Set the maximum number of items: 10000000
  -v <value_size>  Set the maximum value size: 4096
  -d <ssd_num>     Set the number of SSDs: 4
  -c <config_file> Set the SPDK JSON config file: config.json
  -i <io_num>      Set the maximum number of concurrent I/Os: 2048
  -I <copy_concur> Set the copy concurrency: 32
  -T <thread_num>  Set the number of threads for handling RDMA requests: 3
  -s <etcd_ip>     Set the etcd's IP: 127.0.0.1
  -P <etcd_port>   Set the etcd's port: 2379
  -l <local_ip>    Set the local IP for remote connects: 192.168.1.13
  -p <local_port>  Set the local port for remote connects: 9000
  -M <ring_num>    Set the number of hash ring (must be same within a cluster): 120
  -m <vid_per_ssd> Set the number of VID per SSD (must be same within a cluster): 30
  -R <rpl_num>     Set the number of replica (must be same within a cluster): 3
```
#### Client

```[bash]
# ring_ycsb_client/kv_ring_ycsb_client -h

Program options:
  -h               Display this help message
  -c <config_file> Set the SPDK JSON config file: config.json
  -w <workload_file> Set the YCSB workload file: workloada.spec
  -P <producer_num> Set the number of YCSB threads : 1
  -i <io_num>      Set the number of concurrent I/Os: 32
  -s <etcd_ip>     Set the etcd's IP: 192.168.1.13
  -p <etcd_port>   Set the etcd's port: 2379
  -T <thread_num>  Set the number of threads for handling RDMA requests: 2
  -I <stat_interval> Set the statistics interval (set to -1 to disable): -1
  -R               Perform sequential read operations
  -W               Perform sequential write operations
  -D               Perform delete operations
  -F               Perform fill operations
```