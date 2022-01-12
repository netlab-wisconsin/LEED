#!/bin/bash
go build -buildmode=c-shared -o kv_etcd
mv -f kv_etcd /usr/local/lib/libkv_etcd.so
mv -f kv_etcd.h /usr/local/include/