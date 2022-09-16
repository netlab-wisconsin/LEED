package main

//#include <stdint.h>
//#include <stdbool.h>
//enum kv_etcd_msg_type {KV_ETCD_MSG_PUT, KV_ETCD_MSG_DEL};
//typedef void (*kv_etcd_msg_handler)(enum kv_etcd_msg_type msg, const char * key, uint32_t key_len, const void * val, uint32_t val_len);
//static void _msg_hdl_wrapper(kv_etcd_msg_handler h, uint32_t msg_type, _GoString_ key, _GoString_ val) {
//	h((enum kv_etcd_msg_type)msg_type, key.p, key.n, val.p, val.n);
//}
import "C"
import (
	"context"
	"log"
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
	ctx      context.Context
	cli      *clientv3.Client
	msgHdl   C.kv_etcd_msg_handler
	funcChan chan func() error
	DebugMod = false
)

//export kvEtcdLeaseCreate
func kvEtcdLeaseCreate(ttl C.uint32_t, keepalive C.bool) C.uint64_t { //sync
	lease, err := cli.Grant(ctx, int64(ttl))
	if err != nil {
		log.Fatalln("unable to create the lease")
	}
	if keepalive {
		leaseCh, err := cli.KeepAlive(ctx, lease.ID)
		if err != nil {
			log.Fatalln("unable to keepalive")
		}
		go func() {
			for range leaseCh {
			}
		}()
	}
	return C.uint64_t(lease.ID)
}

//export kvEtcdLeaseRevoke
func kvEtcdLeaseRevoke(leaseID C.uint64_t) { //async
	leaseId := clientv3.LeaseID(leaseID)
	funcChan <- func() error {
		ttl, err := cli.TimeToLive(ctx, leaseId)
		if err != nil {
			return err
		}
		time.Sleep(time.Duration(ttl.TTL) * time.Second)
		_, err = cli.Revoke(ctx, leaseId)
		return err
	}
}

//export kvEtcdPut
func kvEtcdPut(key *C.char, val unsafe.Pointer, valLen C.uint32_t, leaseID *C.uint64_t) { //async
	k, v := C.GoString(key), C.GoStringN((*C.char)(val), C.int(valLen))
	if leaseID == nil {
		funcChan <- func() error {
			_, err := cli.Put(ctx, k, v)
			return err
		}
	} else {
		lease := clientv3.WithLease(clientv3.LeaseID(*leaseID))
		funcChan <- func() error {
			_, err := cli.Put(ctx, k, v, lease)
			return err
		}
	}

}

//export kvEtcdDel
func kvEtcdDel(key *C.char) { //async
	k := C.GoString(key)
	funcChan <- func() error {
		_, err := cli.Delete(ctx, k)
		return err
	}
}

func onKeyChange(kv *mvccpb.KeyValue, msgType mvccpb.Event_EventType) {
	if DebugMod {
		println(msgType, string(kv.Key[:]))
	}
	if msgHdl == nil {
		return
	}
	C._msg_hdl_wrapper(msgHdl, C.uint32_t(msgType), string(kv.Key[:]), string(kv.Value[:]))
}

//export kvEtcdInit
func kvEtcdInit(ip, port *C.char, _msgHdl C.kv_etcd_msg_handler) C.int { //sync
	funcChan = make(chan func() error, 4096)
	for i := 0; i < 16; i++ {
		go func() {
			for f := range funcChan {
				if err := f(); err != nil {
					log.Fatalln(err)
				}
			}
		}()
	}
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
	msgHdl = _msgHdl
	rch := cli.Watch(ctx, "", clientv3.WithPrefix())
	gr, err := cli.Get(ctx, "", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return -2
	}
	for _, x := range gr.Kvs {
		onKeyChange(x, mvccpb.PUT)
	}
	go func() {
		for resp := range rch {
			for _, ev := range resp.Events {
				onKeyChange(ev.Kv, ev.Type)
			}
		}
	}()
	return 0
}

//export kvEtcdFini
func kvEtcdFini() C.int { //sync
	close(funcChan)
	err := cli.Close()
	if err != nil {
		return -1
	} else {
		return 0
	}
}

func main() {
	//tests
	DebugMod = true
	kvEtcdInit(C.CString("127.0.0.1"), C.CString("2379"), nil)
	val := unsafe.Pointer(C.CString("a\x00aaaaaaaaaaaa"))
	nodeId := C.CString("10.0.0.1:5000")
	nodeId2 := C.CString("10.0.0.2:5000")
	leaseID := kvEtcdLeaseCreate(5, true)
	kvEtcdPut(nodeId, val, 8, &leaseID)
	kvEtcdPut(nodeId2, val, 8, nil)
	time.Sleep(5 * time.Second)
	kvEtcdDel(nodeId2)
	time.Sleep(5 * time.Second)
	println("revoke the lease")
	kvEtcdLeaseRevoke(leaseID)
	time.Sleep(8 * time.Second)
	kvEtcdFini()
}
