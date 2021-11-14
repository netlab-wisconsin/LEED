# spdk kv

### TODO List
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

### spdk bugs

https://review.spdk.io/gerrit/c/spdk/spdk/+/4859/2/lib/nvme/nvme_qpair.c#670

### known issues

+ circular log fetch: too many fragmentation during fetching data.