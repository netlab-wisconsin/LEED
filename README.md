# LEED

[![DOI](https://zenodo.org/badge/407594373.svg)](https://zenodo.org/badge/latestdoi/407594373)

LEED is a distributed, replicated, and persistent key-value store over an array of SmartNIC JBOFs. Our key ideas to tackle the unique challenges induced by a SmartNIC JBOF are: trading excessive I/O bandwidth for scarce SmartNIC core computing cycles and memory capacity; making scheduling decisions as early as possible to streamline the request execution flow. LEED systematically revamps the software stack and proposes techniques across per-SSD, intra-JBOF, and inter-JBOF levels. Our prototyped system based on Broadcom Stingray outperforms existing solutions that use beefy server JBOFs and wimpy embedded storage nodes by 4.2X/3.8X and 17.5X/19.1X in terms of requests per Joule for 256B/1KB key-value objects.

## Build & Run Instractions
Please follow this [guide](/spdk/app/leed/README.md).
