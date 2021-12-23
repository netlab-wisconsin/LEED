# spdk kv

### TODO List

+ using mempool in data-path

#### DataStore
+ ~~bucket log compact~~

    + ~~bucket log bit map~~ 
    + ~~concurrent bucket log compact~~
    + ~~bucket log prefetch buffer~~

+ ~~delete~~
+ ~~data store benchmark~~
+ ~~rewrite value log (using circular log)~~
+ ~~value log compact~~

    + ~~index log~~
    + index log update buffer

+ don't copy during value log read
+ write small value to bucket log
+ set: write bucket/value at the same time
+ bucket cache for value compression
+ dynamic queue length

#### RDMA
+ RDMA_READ
+ shared recieve queue

#### replication
+ Control-plane

    + ectd

### spdk bugs

https://review.spdk.io/gerrit/c/spdk/spdk/+/4859/2/lib/nvme/nvme_qpair.c#670

### known issues

+ ~~circular log fetch: too many fragmentation during fetching data.~~