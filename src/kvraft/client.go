package raftkv

import (
	"labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	lastSuccessServer int

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
	// You'll have to add code here.
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
	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}
	respCh := make(chan RespMsg)
	ck.Call("KVServer.Get", args, respCh)

	var resp RespMsg
	select {
	case resp = <-respCh:
		return resp.msg
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
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}

	respCh := make(chan RespMsg)
	ck.Call("KVServer.PutAppend", args, respCh)

	var resp RespMsg
	select {
	case resp = <-respCh:
		return
	}
}

func (ck *Clerk) Call(method string, args interface{}, respCh chan RespMsg) {
	ck.mu.Lock()
	prefer := ck.lastSuccessServer
	ck.mu.Unlock()
	ret := ""

	defer func() {
		ck.mu.Lock()
		ck.lastSuccessServer = prefer
		ck.mu.Unlock()

		respCh <- RespMsg{
			msg: ret,
		}
	}()

	// todo 可能异步同时多个重试好一些
	// keeps trying forever in the face of all other errors.
	offset := int64(prefer)
	for {
		id := int(offset % int64(len(ck.servers)))
		var reply GetReply
		if ck.servers[id].Call(method, args, &reply) {
			// Get returns "" if the key does not exist.
			if method == "KVServer.Get" && reply.Err == ErrNoKey {
				prefer = id
				return
			}
			if !reply.WrongLeader && len(reply.Err) == 0 {
				prefer = id
				ret = reply.Value
				return
			}
		}

		offset = nrand()

		DPrintf("Clerk.Call fail: id=%d, need retry", id)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
