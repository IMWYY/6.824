package raftkv

import (
	"bytes"
	"github.com/luci/go-render/render"
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

// TODO 遇到的问题和解决方法
// 1. 对于一个client发送了请求到leader，但是在处理过程中leader变成follower的情况，有两种判断方法：
//   a. 在返回rpc之前检测term是否发生变化
//   b. 在applyMessage的时候，检查同一个logIndex是否出现了不同的请求。
//  这里用了第二种方法，所以利用一个logIndex2ReqId的映射
// 2. 为了应用请求出重，这里在server保存了clientId->reqId的映射，只处理reqId递增的请求

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
	err   Err
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxRaftState int // snapshot if log grows this big
	persister    *raft.Persister

	exitCh         chan struct{}
	pendingReq     map[int]chan NotifyApplyMsg // logIndex -> channel
	kvStore        map[string]string
	reqIdCache     map[int64]int64 // clientId -> reqId, provided that one client one rpc at a time
	logIndex2ReqId map[int]int64   // logIndex -> reqId, use to detect leadership change
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	s := time.Now()
	defer func() {
		DPrintf("KVServer(%d).Get latency=%v, args=%v, reply=%v", kv.me, time.Since(s).Nanoseconds()/1e6,
			render.Render(args), render.Render(reply))
	}()

	reply.Err, reply.Value = kv.start(Op{
		OpType:   OpTypeGet,
		Key:      args.Key,
		ReqId:    args.ReqId,
		ClientId: args.ClientId,
	})

	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	s := time.Now()
	defer func() {
		DPrintf("KVServer(%d).PutAppend latency=%v args=%v, reply=%v", kv.me, time.Since(s).Nanoseconds()/1e6,
			render.Render(args), render.Render(reply))
	}()

	reply.Err, _ = kv.start(Op{
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

func (kv *KVServer) start(op Op) (Err, string) {
	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("KVServer(%d) non-leader,", kv.me)
		return ErrWrongLeader, ""
	}

	done := make(chan NotifyApplyMsg, 1)
	kv.mu.Lock()
	if _, ok := kv.pendingReq[logIndex]; ok {
		kv.mu.Unlock()
		return ErrWrongLeader, ""
	} else {
		kv.pendingReq[logIndex] = done
	}
	kv.logIndex2ReqId[logIndex] = op.ReqId
	kv.mu.Unlock()

	DPrintf("KVServer(%d) waiting ApplyMsg logIndex(%d), Op=%v", kv.me, logIndex, render.Render(op))

	var msg NotifyApplyMsg
	select {
	case msg = <-done:
		return msg.err, msg.value
	case <-time.After(RequestTimeOut):
		kv.mu.Lock()
		delete(kv.pendingReq, logIndex)
		kv.mu.Unlock()
		return ErrTimeout, ""
	case <-kv.exitCh:
		return ErrCrash, ""
	}
}

// handle applyMsg
func (kv *KVServer) run() {
	var applyMsg raft.ApplyMsg
	for {
		select {
		case applyMsg = <-kv.applyCh:
			kv.mu.Lock()

			if applyMsg.UseSnapshot {
				// handle snapshot 返回的snapshot的data会包含raft log的info
				var includedIndex, includedTerm int
				dec := labgob.NewDecoder(bytes.NewBuffer(applyMsg.Snapshot))
				kv.kvStore = make(map[string]string)
				kv.reqIdCache = make(map[int64]int64)
				kv.logIndex2ReqId = make(map[int]int64)
				if dec.Decode(&includedIndex) != nil ||
					dec.Decode(&includedTerm) != nil ||
					dec.Decode(&kv.kvStore) != nil ||
					dec.Decode(&kv.reqIdCache) != nil ||
					dec.Decode(&kv.logIndex2ReqId) != nil {
					panic("god decode error")
				}

				// todo need to replay logs

				DPrintf("KVServer(%d) includedIndex(%d) includedTerm(%d) restore snapshot", kv.me, includedIndex, includedTerm)
			} else {
				cmd := applyMsg.Command.(Op)

				// 1. deduplicate and apply log message
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

				// apply get message
				notifyMsg := NotifyApplyMsg{err: OK, value: ""}
				if cmd.OpType == OpTypeGet {
					if v, ok := kv.kvStore[cmd.Key]; ok {
						notifyMsg.value = v
					} else {
						notifyMsg.err = ErrNoKey
					}
				}

				// 2. return rpc request
				// to detect that it has lost leadership,
				// 	a. by noticing that a different request has appeared at the index returned by Start(),
				// 	b. or that Raft's term has changed
				if v, ok := kv.pendingReq[applyMsg.Index]; ok {
					if kv.logIndex2ReqId[applyMsg.Index] == cmd.ReqId {
						v <- notifyMsg
					} else {
						v <- NotifyApplyMsg{err: ErrWrongLeader, value: ""}
					}
					delete(kv.logIndex2ReqId, applyMsg.Index)
					delete(kv.pendingReq, applyMsg.Index)
				}

				// 3. snapshot if size more than a fixed size
				if kv.maxRaftState > 0 && kv.persister.RaftStateSize() > kv.maxRaftState {
					var b bytes.Buffer
					enc := labgob.NewEncoder(&b)
					enc.Encode(kv.kvStore)
					enc.Encode(kv.reqIdCache)
					enc.Encode(kv.logIndex2ReqId)
					// 这里一定要用goroutine 否则会产生死锁
					// 死锁条件：applyMsg生产时会先获取锁, 消费时又去获取锁，导致chan不能被消费，从而导致生产被阻塞
					go kv.rf.StartSnapshot(b.Bytes(), applyMsg.Index)
					DPrintf("KVServer(%d) logIndex(%d) reach maxRaftState, need snapshot", kv.me, applyMsg.Index)
				}

				DPrintf("KVServer(%d) apply key(%v), value(%v), logIndex(%d) receive applyMsg(%v)", kv.me, cmd.Key,
					kv.kvStore[cmd.Key], applyMsg.Index, render.Render(applyMsg))
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
	kv.persister = persister

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
