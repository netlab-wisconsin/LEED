# spdk kv

### TODO List

+ ~~using mempool in data-path~~

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
+ ~~set: write bucket/value at the same time~~
+ ~~bucket cache~~
+ dynamic queue length
+ optimize value log compaction
+ support commit & rollback

#### RDMA
+ ~~shared recieve queue~~
+ ~~multi-thread server~~
+ ~~flow control~~

#### replication
+ ~~Control-plane~~

    + ~~ectd~~

+ ~~data-distribution~~
+ node join & leave
    + ~~operations flow~~

    + pre_copy

    + ~~new kv data structure in the etcd~~

+ ~~chain replication~~

    + ~~set & del~~

    + ~~read~~

    + remove the requests shipping?

### spdk bugs

https://review.spdk.io/gerrit/c/spdk/spdk/+/4859/2/lib/nvme/nvme_qpair.c#670

### known issues

+ ~~circular log fetch: too many fragmentation during fetching data.~~