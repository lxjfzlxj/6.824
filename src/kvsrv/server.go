package kvsrv

import (
	"log"
	"sync"
	// "fmt"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mp map[string]string
	mu sync.Mutex
	taskStatus map[int64]string
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.mp[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// fmt.Printf("put %d\n",args.Id)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exist := kv.taskStatus[args.Id]
	if !exist {
		kv.mp[args.Key] = args.Value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// fmt.Printf("append %d\n",args.Id)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	record, exist := kv.taskStatus[args.Id]
	if !exist {
		reply.Value = kv.mp[args.Key]
		value := reply.Value + args.Value
		kv.mp[args.Key] = value
		kv.taskStatus[args.Id] = reply.Value
	} else {
		reply.Value = record
	}
}

func (kv *KVServer) ReportFinished(args *FinishedMessageArgs, reply *EmptyStruct) {
	// fmt.Printf("%d finished\n",args.Id)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exist := kv.taskStatus[args.Id]
	if exist {
		delete(kv.taskStatus, args.Id)
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.mp = make(map[string]string)
	kv.taskStatus = make(map[int64]string)
	return kv
}
