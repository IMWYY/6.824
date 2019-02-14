package shardkv

import (
	"bytes"
	"github.com/luci/go-render/render"
	"labgob"
	"labrpc"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

const RequestTimeOut = 1 * time.Second

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxRaftState int // snapshot if log grows this big

	persister *raft.Persister
	mck       *shardmaster.Clerk
	conf      shardmaster.Config

	exitCh         chan struct{}
	pendingReq     map[int]chan NotifyApplyMsg // logIndex -> channel
	kvStore        map[string]string
	reqIdCache     map[int64]int64 // clientId -> reqId, provided that one client one rpc at a time
	logIndex2ReqId map[int]int64   // logIndex -> reqId, use to detect leadership change
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	defer func() {
		DPrintf("ShardKV(%d).Get args=%v, reply=%v", kv.me, render.Render(args), render.Render(reply))
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer func() {
		DPrintf("ShardKV(%d).PutAppend args=%v, reply=%v", kv.me, render.Render(args), render.Render(reply))
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

func (kv *ShardKV) start(op Op) (Err, string) {
	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("ShardKV(%d) non-leader,", kv.me)
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

	DPrintf("ShardKV(%d) waiting ApplyMsg logIndex(%d), Op=%v", kv.me, logIndex, render.Render(op))

	var msg NotifyApplyMsg
	select {
	case msg = <-done:
		return msg.err, msg.value
	case <-time.After(RequestTimeOut):
		return ErrTimeout, ""
	case <-kv.exitCh:
		return ErrCrash, ""
	}
}

func (kv *ShardKV) run() {
	var applyMsg raft.ApplyMsg
	for {
		select {
		case applyMsg = <-kv.applyCh:
			kv.mu.Lock()

			if applyMsg.UseSnapshot {
				// todo check snapshot state
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

				DPrintf("ShardKV(%d) includedIndex(%d) includedTerm(%d) restore snapshot", kv.me, includedIndex, includedTerm)
			} else {
				cmd := applyMsg.Command.(Op)

				// 1. apply log message and deduplicate
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
					DPrintf("ShardKV(%d) logIndex(%d) reach maxRaftState, need snapshot", kv.me, applyMsg.Index)
				}

				DPrintf("ShardKV(%d) apply key(%v), value(%v), logIndex(%d) receive applyMsg(%v)", kv.me, cmd.Key,
					kv.kvStore[cmd.Key], applyMsg.Index, render.Render(applyMsg))
			}
			kv.mu.Unlock()
		case <-kv.exitCh:
			return
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.exitCh)
	DPrintf("ShardKV(%d) exit", kv.me)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.persister = persister
	kv.exitCh = make(chan struct{})
	kv.kvStore = make(map[string]string)
	kv.reqIdCache = make(map[int64]int64)
	kv.logIndex2ReqId = make(map[int]int64)
	kv.pendingReq = make(map[int]chan NotifyApplyMsg)
	kv.conf = shardmaster.Config{}

	// todo async or not
	go func() {
		kv.conf = kv.mck.Query(-1)
	}()
	go kv.run()

	return kv
}
