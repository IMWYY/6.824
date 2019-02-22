package shardmaster

import (
	"log"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c Config) Copy() Config {
	res := Config{
		Num:    c.Num,
		Groups: make(map[int][]string),
		Shards: [NShards]int{},
	}

	for k, v := range c.Groups {
		res.Groups[k] = v
	}
	for k, v := range c.Shards {
		res.Shards[k] = v
	}
	return res

}

type Err string

const (
	OK             = "OK"
	ErrTimeout     = "ErrTimeout"
	ErrWrongLeader = "ErrWrongLeader"
	ErrCrash       = "ErrCrash"

	OpTypeJoin  = "Join"
	OpTypeLeave = "Leave"
	OpTypeMove  = "Move"
	OpTypeQuery = "Query"

	Debug = 0
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ReqId    int64
	ClientId int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	ReqId    int64
	ClientId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ReqId    int64
	ClientId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	ReqId    int64
	ClientId int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
