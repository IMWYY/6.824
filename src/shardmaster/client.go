package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

const RetryInterval = 100 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	clientId          int64
	nextReqId         int64
	lastSuccessServer int

	mu sync.Mutex
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
	// Your code here.
	ck.clientId = nrand()
	ck.nextReqId = nrand() % 10000000001
	ck.lastSuccessServer = 0
	return ck
}

func (ck *Clerk) getReqBase() (int, int64) {
	ck.mu.Lock()
	defer func() {
		ck.nextReqId ++
		ck.mu.Unlock()
	}()
	return ck.lastSuccessServer, ck.nextReqId
}

func (ck *Clerk) Query(num int) Config {
	offset, reqId := ck.getReqBase()
	defer func() {
		ck.mu.Lock()
		ck.lastSuccessServer = offset
		ck.mu.Unlock()
	}()

	args := &QueryArgs{
		Num:   num,
		ReqId: reqId,
		ClientId: ck.clientId,
	}
	for {
		id := offset % len(ck.servers)
		var reply QueryReply
		ok := ck.servers[id].Call("ShardMaster.Query", args, &reply)
		if ok && !reply.WrongLeader && (reply.Err == OK || len(reply.Err) == 0) {
			return reply.Config
		}
		offset ++
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	offset, reqId := ck.getReqBase()
	defer func() {
		ck.mu.Lock()
		ck.lastSuccessServer = offset
		ck.mu.Unlock()
	}()

	args := &JoinArgs{
		Servers: servers,
		ReqId:   reqId,
		ClientId: ck.clientId,
	}
	for {
		id := offset % len(ck.servers)
		var reply JoinReply
		ok := ck.servers[id].Call("ShardMaster.Join", args, &reply)
		if ok && !reply.WrongLeader && (reply.Err == OK || len(reply.Err) == 0) {
			return
		}
		offset ++
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	offset, reqId := ck.getReqBase()
	defer func() {
		ck.mu.Lock()
		ck.lastSuccessServer = offset
		ck.mu.Unlock()
	}()
	args := &LeaveArgs{
		GIDs:  gids,
		ReqId: reqId,
		ClientId: ck.clientId,
	}

	for {
		id := offset % len(ck.servers)
		var reply LeaveReply
		ok := ck.servers[id].Call("ShardMaster.Leave", args, &reply)
		if ok && !reply.WrongLeader && (reply.Err == OK || len(reply.Err) == 0) {
			return
		}
		offset ++
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	offset, reqId := ck.getReqBase()
	defer func() {
		ck.mu.Lock()
		ck.lastSuccessServer = offset
		ck.mu.Unlock()
	}()
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ReqId:    reqId,
		ClientId: ck.clientId,
	}
	for {
		id := offset % len(ck.servers)
		var reply MoveReply
		ok := ck.servers[id].Call("ShardMaster.Move", args, &reply)
		if ok && !reply.WrongLeader && (reply.Err == OK || len(reply.Err) == 0) {
			return
		}
		offset ++
		time.Sleep(RetryInterval)
	}
}
