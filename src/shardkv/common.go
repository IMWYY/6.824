package shardkv

import (
	"crypto/rand"
	"log"
	"math/big"
	"shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrTimeout     = "ErrTimeout"
	ErrWrongLeader = "ErrWrongLeader"
	ErrCrash       = "ErrCrash"
	ErrWaitNewData = "ErrWaitingNewData"
	ErrUnequalNum  = "ErrUnequalNum"

	OpTypeGet         = "Get"
	OpTypePut         = "Put"
	OpTypeAppend      = "Append"
	OpTypeConfUpdate  = "ConfUpdate"
	OpTypeMigrateData = "MigrateData"

	Debug = 0
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ReqId    int64
	ClientId int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key      string
	ReqId    int64
	ClientId int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type MigrateDataArgs struct {
	ClientId int64
	ReqId    int64
	ConfNum  int
	ToGid    int
	Shards   []int
	Data     map[string]string
	ReqCache map[int64]int64
}

type MigrateData struct {
	Servers  []string
	DataArgs MigrateDataArgs
}

type MigrateDataReply struct {
	WrongLeader bool
	Err         Err
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
