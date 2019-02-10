package raftkv

import (
	"github.com/luci/go-render/render"
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

const (
	RequestTimeOut = 1 * time.Second
)

type Op struct {
	OpType   string
	Key      string
	Value    string
	ReqId    int64
	ClientId int64
}

type NotifyApplyMsg struct {
	err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxRaftState int // snapshot if log grows this big

	exitCh         chan struct{}
	pendingReq     map[int]chan NotifyApplyMsg // logIndex -> channel
	kvStore        map[string]string
	reqIdCache     map[int64]int64 // clientId -> reqId, provided that one client one rpc at a time
	logIndex2ReqId map[int]int64   // logIndex -> reqId, use to detect leadership change
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	defer func() {
		DPrintf("KVServer(%d).Get args=%v, reply=%v", kv.me, render.Render(args), render.Render(reply))
	}()

	reply.Err = kv.start(Op{
		OpType:   OpTypeGet,
		Key:      args.Key,
		ReqId:    args.ReqId,
		ClientId: args.ClientId,
	})

	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.kvStore[args.Key]; ok {
		reply.Value = v
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer func() {
		DPrintf("KVServer(%d).PutAppend args=%v, reply=%v", kv.me, render.Render(args), render.Render(reply))
	}()

	reply.Err = kv.start(Op{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ReqId:    args.ReqId,
		ClientId: args.ClientId,
	})

	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}
}

func (kv *KVServer) start(op Op) Err {
	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("KVServer(%d) non-leader,", kv.me)
		return ErrWrongLeader
	}

	done := make(chan NotifyApplyMsg, 1)
	kv.mu.Lock()
	if _, ok := kv.pendingReq[logIndex]; ok {
		kv.mu.Unlock()
		return ErrWrongLeader
	} else {
		kv.pendingReq[logIndex] = done
	}
	kv.logIndex2ReqId[logIndex] = op.ReqId
	kv.mu.Unlock()

	DPrintf("KVServer(%d) waiting ApplyMsg logIndex(%d), Op=%v", kv.me, logIndex, render.Render(op))

	var msg NotifyApplyMsg
	select {
	case msg = <-done:
		return msg.err
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
			kv.mu.Lock()
			cmd := applyMsg.Command.(Op)
			if kv.reqIdCache[cmd.ClientId] < cmd.ReqId {
				if cmd.OpType == OpTypePut {
					kv.kvStore[cmd.Key] = cmd.Value
				} else if cmd.OpType == OpTypeAppend {
					if v, ok := kv.kvStore[cmd.Key]; ok {
						kv.kvStore[cmd.Key] = v + cmd.Value
					} else {
						kv.kvStore[cmd.Key] = cmd.Value
					}
				}
				kv.reqIdCache[cmd.ClientId] = cmd.ReqId
			}

			// to detect that it has lost leadership,
			// 1. by noticing that a different request has appeared at the index returned by Start(),
			// 2. or that Raft's term has changed
			if v, ok := kv.pendingReq[applyMsg.Index]; ok {
				if kv.logIndex2ReqId[applyMsg.Index] == cmd.ReqId {
					v <- NotifyApplyMsg{err: OK}
				} else {
					v <- NotifyApplyMsg{err: ErrWrongLeader}
				}
				delete(kv.logIndex2ReqId, applyMsg.Index)
				delete(kv.pendingReq, applyMsg.Index)
			}
			kv.mu.Unlock()
			DPrintf("KVServer(%d) apply key(%v), value(%v), logIndex(%d) receive applyMsg(%v)", kv.me, cmd.Key,
				kv.kvStore[cmd.Key], applyMsg.Index, render.Render(applyMsg))
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

	kv.exitCh = make(chan struct{})
	kv.kvStore = make(map[string]string)
	kv.reqIdCache = make(map[int64]int64)
	kv.logIndex2ReqId = make(map[int]int64)
	kv.pendingReq = make(map[int]chan NotifyApplyMsg)
	go kv.run()
	return kv
}
