#ifndef _MEMERY_OPERATION_H_
#define _MEMERY_OPERATION_H_
#include<stdlib.h>
#include<string.h>
//TODO: using SPDK

#define kv_memcpy(dst,src,n) memcpy(dst,src,n)
#define kv_memcmp8(dst,src,n) memcmp(dst,src,n)
#define kv_memset(s,c,n) memset(s,c,n)
#define kv_malloc(size) malloc(size)
#define kv_calloc(nmemb, size) calloc(nmemb, size)
#define kv_free(ptr)  free(ptr) 
#endif