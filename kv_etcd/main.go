package main

//#include <stdint.h>
//#include <string.h>
//#include <stdlib.h>
//struct kv_vid {
//    uint32_t ssd_id;
//    char vid[20];
//#define KV_VID_EMPTY UINT32_MAX
//} __attribute__((packed));
//
//struct kv_node_info {
//    char rdma_ip[16];
//    char rdma_port[8];
//    uint32_t msg_type;
//    uint32_t vid_num;
//    struct kv_vid vids[0];
//#define KV_NODE_INFO_READ (0)
//#define KV_NODE_INFO_CREATE (1)
//#define KV_NODE_INFO_DELETE (2)
//} __attribute__((packed));
//
////free *info in handler
//typedef void (*kv_etcd_node_handler)(char * node_id, struct kv_node_info *info); //create or delete
//typedef void (*kv_etcd_vid_handler)(uint32_t vid_index, struct kv_vid *vid); //update
//static struct kv_vid * kv_etcd_get_vid(struct kv_node_info *info,uint32_t index){
//	return info->vids+index;
//}
//static struct kv_node_info * kv_node_info_alloc(uint32_t vid_num){
//	return malloc(sizeof(struct kv_node_info) + vid_num * sizeof(struct kv_vid));
//}
//static void kv_etcd_node_handler_wrapper(kv_etcd_node_handler h, char * node_id, struct kv_node_info *info){
//	h(node_id,info);
//}
import "C"

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"log"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

var (
	dialTimeout      = 2 * time.Second
	autoSyncInterval = 5 * time.Second
	ctx              context.Context
	cli              *clientv3.Client
	kv               clientv3.KV
	leaseID          clientv3.LeaseID
	nodeHandler      C.kv_etcd_node_handler
	vidHandler       C.kv_etcd_vid_handler
	vidNum           *int
)

//export kvEtcdCreateNode
func kvEtcdCreateNode(CNodeId *C.char, info *C.struct_kv_node_info, ttl C.uint64_t) {
	gr, _ := kv.Get(ctx, "vid_num")
	if len(gr.Kvs) == 1 {
		vidNum, _ := strconv.Atoi(string(gr.Kvs[0].Value[:]))
		if vidNum != int(info.vid_num) {
			log.Fatalln("different vid_num in same cluster!")
		}
	} else {
		_, _ = kv.Put(ctx, "vid_num", strconv.Itoa(int(info.vid_num)))
	}
	nodeId := uuid.New()
	key := "node/" + nodeId.String() + "/"
	if CNodeId != nil {
		_CNodeId := C.CBytes(nodeId[:])
		C.memcpy(unsafe.Pointer(CNodeId), _CNodeId, 16)
		C.free(_CNodeId)
	}
	fmt.Println(key)
	lease, _ := cli.Grant(ctx, int64(ttl))
	leaseID = lease.ID
	Ops := []clientv3.Op{
		clientv3.OpPut(key+"rdma_ip", C.GoString(&info.rdma_ip[0]), clientv3.WithLease(leaseID)),
		clientv3.OpPut(key+"rdma_port", C.GoString(&info.rdma_port[0]), clientv3.WithLease(leaseID))}
	for i := 0; i < int(info.vid_num); i++ {
		vidKey := fmt.Sprintf("%svid/%d", key, i)
		vid := unsafe.Pointer(C.kv_etcd_get_vid(info, C.uint32_t(i)))
		Ops = append(Ops, clientv3.OpPut(vidKey, C.GoStringN((*C.char)(vid), C.int(24)), clientv3.WithLease(leaseID)))
	}
	_, _ = kv.Txn(ctx).Then(Ops...).Commit()
}

//export kvEtcdKeepAlive
func kvEtcdKeepAlive() {
	_, _ = cli.KeepAliveOnce(ctx, leaseID)
}

func parseNodeInfo(info *C.struct_kv_node_info, keys []string, value []byte) {
	CValue := C.CBytes(value)
	switch keys[0] {
	case "rdma_ip":
		C.memcpy(unsafe.Pointer(&info.rdma_ip[0]), CValue, C.size_t(len(value)))
	case "rdma_port":
		C.memcpy(unsafe.Pointer(&info.rdma_port[0]), CValue, C.size_t(len(value)))
	case "vid":
		i, _ := strconv.Atoi(keys[1])
		vid := C.kv_etcd_get_vid(info, C.uint32_t(i))
		C.memcpy(unsafe.Pointer(vid), CValue, 24)
	}
	C.free(unsafe.Pointer(CValue))
}
func sendNodeInfo(Kvs []*mvccpb.KeyValue, msgTypes []int) {
	if vidNum == nil {
		gr, _ := kv.Get(ctx, "vid_num")
		if len(gr.Kvs) != 1 {
			return
		}
		vidNum = new(int)
		*vidNum, _ = strconv.Atoi(string(gr.Kvs[0].Value[:]))
	}
	nodeMap := make(map[uuid.UUID]*C.struct_kv_node_info)
	for i, x := range Kvs {
		key := strings.Split(string(x.Key[:]), "/")
		nodeID, _ := uuid.Parse(key[1])
		if _, ok := nodeMap[nodeID]; !ok {
			nodeMap[nodeID] = C.kv_node_info_alloc(C.uint32_t(*vidNum))
			info := nodeMap[nodeID]
			info.vid_num = C.uint32_t(*vidNum)
			info.msg_type = C.uint32_t(msgTypes[i])
		}
		if info := nodeMap[nodeID]; info.msg_type != C.KV_NODE_INFO_DELETE {
			parseNodeInfo(info, key[2:], x.Value)
		}
	}
	for i, info := range nodeMap {
		if nodeHandler != nil {
			CNodeId := C.CBytes(i[:])
			C.kv_etcd_node_handler_wrapper(nodeHandler, (*C.char)(CNodeId), info)
			C.free(CNodeId)
		} else {
			C.free(unsafe.Pointer(info))
		}
	}
}

//export kvEtcdInit
func kvEtcdInit(ip, port *C.char, _nodeHandler C.kv_etcd_node_handler, _vidHandler C.kv_etcd_vid_handler) {
	ctx = context.Background()
	cli, _ = clientv3.New(clientv3.Config{
		DialTimeout:      dialTimeout,
		Endpoints:        []string{C.GoString(ip) + ":" + C.GoString(port)},
		AutoSyncInterval: autoSyncInterval,
	})
	kv = clientv3.NewKV(cli)
	nodeHandler = _nodeHandler
	vidHandler = _vidHandler
	rch := cli.Watch(ctx, "node/", clientv3.WithPrefix())
	go func() {
		for resp := range rch {
			Kvs := make([]*mvccpb.KeyValue, len(resp.Events))
			msgTypes := make([]int, len(resp.Events))
			for i, ev := range resp.Events {
				Kvs[i] = ev.Kv
				if ev.Type == mvccpb.PUT {
					msgTypes[i] = int(C.KV_NODE_INFO_CREATE)
				} else {
					msgTypes[i] = int(C.KV_NODE_INFO_DELETE)
				}
				//fmt.Printf("%s %q : %q\n", ev.Type, ev.Kv.Key, ev.Kv.Value)
			}
			sendNodeInfo(Kvs, msgTypes)
		}
	}()
	gr, _ := kv.Get(ctx, "node/", clientv3.WithPrefix())
	sendNodeInfo(gr.Kvs, make([]int, len(gr.Kvs)))
}

//export kvEtcdFini
func kvEtcdFini() {
	_ = cli.Close()
}

func main() {
	//"127.0.0.1:2379"127.0.0.1
	var info *C.struct_kv_node_info
	info = (*C.struct_kv_node_info)(C.malloc(1024))
	C.strcpy(&info.rdma_ip[0], C.CString("10.0.0.1"))
	C.strcpy(&info.rdma_port[0], C.CString("9000"))
	info.vid_num = C.uint32_t(4)
	kvEtcdInit(C.CString("127.0.0.1"), C.CString("2379"), nil, nil)
	kvEtcdCreateNode(nil, info, 1)
	go func() {
		for true {
			kvEtcdKeepAlive()
			time.Sleep(300 * time.Millisecond)
		}
	}()
	time.Sleep(300 * time.Second)
}
