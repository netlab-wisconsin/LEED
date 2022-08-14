package main

//#include <stdint.h>
//#include <stdlib.h>
//#include <stdio.h>
//enum kv_etcd_msg_type {KV_ETCD_MSG_PUT, KV_ETCD_MSG_DEL};
//typedef void (*kv_etcd_node_handler)(enum kv_etcd_msg_type msg, const char * node_id, uint32_t id_len, const void * val, uint32_t val_len);
//typedef void (*kv_etcd_ring_handler)(enum kv_etcd_msg_type msg, uint32_t ring_id, const char * node_id, uint32_t id_len, const void * value, uint32_t val_len);
//static void _node_hdl_wrapper(kv_etcd_node_handler h, uint32_t msg_type, _GoString_ node_id, _GoString_ val) {
//	h((enum kv_etcd_msg_type)msg_type, node_id.p, node_id.n, val.p, val.n);
//}
//static void _ring_hdl_wrapper(kv_etcd_ring_handler h, uint32_t msg_type, uint32_t ring_id, _GoString_ node_id, _GoString_ val) {
//	h((enum kv_etcd_msg_type)msg_type, ring_id, node_id.p, node_id.n, val.p, val.n);
//}
import "C"
import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	dialTimeout      = 2 * time.Second
	autoSyncInterval = 5 * time.Second
)

var (
	ctx     context.Context
	cli     *clientv3.Client
	nodeHdl C.kv_etcd_node_handler
	ringHdl C.kv_etcd_ring_handler
)

func eventType2MsgType(t mvccpb.Event_EventType) C.uint32_t {
	return C.uint32_t(t)
}
func onKeyChange(kv *mvccpb.KeyValue, msgType mvccpb.Event_EventType) {
	keys := strings.Split(string(kv.Key[:]), "/")
	if keys[1] == "nodes" && nodeHdl != nil {
		C._node_hdl_wrapper(nodeHdl, eventType2MsgType(msgType), keys[2], string(kv.Value[:]))
	} else if keys[1] == "rings" && ringHdl != nil {
		ringId, _ := strconv.Atoi(keys[2])
		C._ring_hdl_wrapper(ringHdl, eventType2MsgType(msgType), C.uint32_t(ringId), keys[3], string(kv.Value[:]))
	}
}

//export kvEtcdNodeReg
func kvEtcdNodeReg(nodeId *C.char, val unsafe.Pointer, valLen, ttl C.uint32_t) C.int {
	key := "/nodes/" + C.GoString(nodeId)
	fmt.Println(key)
	lease, err := cli.Grant(ctx, int64(ttl))
	if err != nil {
		return -1
	}
	leaseCh, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return -2
	}
	go func() {
		for range leaseCh {
		}
	}()
	if _, err := cli.Put(ctx, key, C.GoStringN((*C.char)(val), C.int(valLen)), clientv3.WithLease(lease.ID)); err != nil {
		return -3
	} else {
		return 0
	}
}

//export kvEtcdVidPut
func kvEtcdVidPut(nodeId *C.char, ringId C.uint32_t, val unsafe.Pointer, valLen C.uint32_t) C.int {
	key := fmt.Sprintf("/rings/%d/%s", uint32(ringId), C.GoString(nodeId))
	fmt.Println(key)
	_, err := cli.Put(ctx, key, C.GoStringN((*C.char)(val), C.int(valLen)))
	if err != nil {
		return -1
	} else {
		return 0
	}
}

//export kvEtcdVidDel
func kvEtcdVidDel(nodeId *C.char, ringId C.uint32_t) C.int {
	key := fmt.Sprintf("/rings/%d/%s", uint32(ringId), C.GoString(nodeId))
	_, err := cli.Delete(ctx, key)
	if err != nil {
		return -1
	} else {
		return 0
	}
}

//export kvEtcdInit
func kvEtcdInit(ip, port *C.char, _nodeHdl C.kv_etcd_node_handler, _ringHdl C.kv_etcd_ring_handler) C.int {
	ctx = context.Background()
	var err error
	cli, err = clientv3.New(clientv3.Config{
		DialTimeout:      dialTimeout,
		Endpoints:        []string{C.GoString(ip) + ":" + C.GoString(port)},
		AutoSyncInterval: autoSyncInterval,
	})
	if err != nil {
		return -1
	}
	nodeHdl = _nodeHdl
	ringHdl = _ringHdl
	rch := cli.Watch(ctx, "/", clientv3.WithPrefix())
	go func() {
		for resp := range rch {
			for _, ev := range resp.Events {
				onKeyChange(ev.Kv, ev.Type)
			}
		}
	}()
	gr, err := cli.Get(ctx, "/", clientv3.WithPrefix())
	if err != nil {
		return -2
	}
	for _, x := range gr.Kvs {
		onKeyChange(x, mvccpb.PUT)
	}
	return 0
}

//export kvEtcdFini
func kvEtcdFini() C.int {
	err := cli.Close()
	if err != nil {
		return -1
	} else {
		return 0
	}
}

func main() {
	//test
	kvEtcdInit(C.CString("127.0.0.1"), C.CString("2379"), nil, nil)
	val := unsafe.Pointer(C.CString("a\x00aaaaaaaaaaaa"))
	nodeId := C.CString("10.0.0.1:5000")
	kvEtcdNodeReg(nodeId, val, 8, 3)
	kvEtcdVidPut(nodeId, 0, val, 4)
	time.Sleep(20 * time.Second)
	kvEtcdFini()
}
