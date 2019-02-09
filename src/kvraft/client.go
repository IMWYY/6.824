package raftkv

import (
	"github.com/luci/go-render/render"
	"labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

const RetryInterval = 150 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	lastSuccessServer int
	nextReqId         int64

	mu sync.Mutex
}

// wrapped Response used in channel
type RespMsg struct {
	msg string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.nextReqId = nrand()%1000001
	ck.lastSuccessServer = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	prefer := ck.lastSuccessServer
	reqId := ck.nextReqId
	ck.nextReqId ++
	ck.mu.Unlock()

	defer func() {
		ck.mu.Lock()
		ck.lastSuccessServer = prefer
		ck.mu.Unlock()
	}()

	offset := 0
	args := GetArgs{
		Key:   key,
		ReqId: reqId,
	}
	for {
		id := (prefer + offset) % len(ck.servers)
		var reply GetReply
		if ck.servers[id].Call("KVServer.Get", &args, &reply) {
			// Get returns "" if the key does not exist.
			if reply.Err == ErrNoKey {
				prefer = id
				return ""
			}
			if !reply.WrongLeader && (len(reply.Err) == 0 || reply.Err == OK) {
				prefer = id
				return reply.Value
			}
			DPrintf("Clerk.Get rpc return false: svcId=%d", id)
		}
		offset ++
		time.Sleep(RetryInterval)
		DPrintf("Clerk.Get fail: svcId=%d, need retry", id)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	prefer := ck.lastSuccessServer
	reqId := ck.nextReqId
	ck.nextReqId ++
	ck.mu.Unlock()
	defer func() {
		ck.mu.Lock()
		ck.lastSuccessServer = prefer
		ck.mu.Unlock()
	}()

	// keeps trying forever in the face of all other errors.
	offset := 0
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		ReqId: reqId,
	}

	DPrintf("Clerk.PutAppend args=%v", render.Render(args))

	for {
		id := (prefer + offset) % len(ck.servers)
		var reply PutAppendReply
		if ck.servers[id].Call("KVServer.PutAppend", &args, &reply) {
			DPrintf("Clerk.PutAppend rpc return: svcId=%d, args=%v, reply=%v", id, render.Render(args), render.Render(reply))
			if !reply.WrongLeader && (len(reply.Err) == 0 || reply.Err == OK) {
				prefer = id
				return
			}
		}
		offset ++
		time.Sleep(RetryInterval)
		DPrintf("Clerk.PutAppend fail: id=%d, need retry", id)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpTypePut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpTypeAppend)
}
