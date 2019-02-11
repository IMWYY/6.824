package raftkv

import "log"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrTimeout     = "ErrTimeout"
	ErrWrongLeader = "ErrWrongLeader"
	ErrCrash       = "ErrCrash"

	OpTypeGet    = "Get"
	OpTypePut    = "Put"
	OpTypeAppend = "Append"

	Debug = 1
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId    int64
	ClientId int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ReqId    int64
	ClientId int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
