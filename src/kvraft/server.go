package raftkv

import (
	"github.com/luci/go-render/render"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const (
	Debug          = 1
	RequestTimeOut = 1 * time.Second
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
	ReqId  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxRaftState int // snapshot if log grows this big

	exitCh     chan struct{}
	pendingReq map[int]chan raft.ApplyMsg // logIndex -> channel
	kvStore    map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer func() {
		DPrintf("KVServer(%d).Get args=%v, reply=%v", kv.me, render.Render(args), render.Render(reply))
	}()

	reply.Err = kv.start(Op{
		OpType: OpTypeGet,
		Key:    args.Key,
		ReqId:  args.ReqId,
	})

	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	return

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.kvStore[args.Key]; ok {
		reply.Value = v
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer func() {
		DPrintf("KVServer(%d).PutAppend args=%v, reply=%v", kv.me, render.Render(args), render.Render(reply))
	}()

	reply.Err = kv.start(Op{
		OpType: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		ReqId:  args.ReqId,
	})

	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
	}
	return
}

// One way to do this is for the server to detect that it has lost leadership,
// by noticing that a different request has appeared at the index returned by Start(),
// or that Raft's term has changed
func (kv *KVServer) start(op Op) Err {
	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("KVServer(%d) non-leader,", kv.me)
		return ErrWrongLeader
	}

	done := make(chan raft.ApplyMsg, 1)
	kv.mu.Lock()
	// 如果index重复 说明当前有分区了 自己不是leader
	if _, ok := kv.pendingReq[logIndex]; ok {
		kv.mu.Unlock()
		DPrintf("logIndex duplicate, logIndex=%d", logIndex)
		return ErrWrongLeader
	} else {
		kv.pendingReq[logIndex] = done
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.pendingReq, logIndex)
		kv.mu.Unlock()
	}()

	select {
	case <-done:
		return OK
	case <-time.After(RequestTimeOut):
		return ErrTimeout
	case <-kv.exitCh:
		return ErrCrash
	}
}

// handle applyMsg
func (kv *KVServer) run() {
	var applyMsg raft.ApplyMsg
	for {
		select {
		case applyMsg = <-kv.applyCh:
			DPrintf("KVServer(%d) receive applyMsg(%v)", kv.me, render.Render(applyMsg))
			kv.mu.Lock()
			// apply to local kv store
			cmd := applyMsg.Command.(Op)
			if cmd.OpType == OpTypePut {
				kv.kvStore[cmd.Key] = cmd.Value
			} else if cmd.OpType == OpTypeAppend {
				if v, ok := kv.kvStore[cmd.Key]; ok {
					kv.kvStore[cmd.Key] = v + cmd.Value
				} else {
					kv.kvStore[cmd.Key] = cmd.Value
				}
			}
			if v, ok := kv.pendingReq[applyMsg.Index]; ok {
				v <- applyMsg
			}
			kv.mu.Unlock()
		case <-kv.exitCh:
			return
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.exitCh)
	DPrintf("KVServer(%d) exit", kv.me)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log.
// if maxraftstate is -1, you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxRaftState

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.exitCh = make(chan struct{})
	kv.kvStore = make(map[string]string)
	kv.pendingReq = make(map[int]chan raft.ApplyMsg)
	go kv.run()
	return kv
}
