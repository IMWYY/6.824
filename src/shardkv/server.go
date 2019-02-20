package shardkv

import (
	"bytes"
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
	ClientId int64
	ReqId    int64
	Key      string
	Value    string

	Conf shardmaster.Config

	ConfNum int
	Data    map[string]string
	Shards  []int
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

	exitCh     chan struct{}
	pendingReq map[int]chan NotifyApplyMsg // logIndex -> channel

	kvStore        map[string]string
	reqIdCache     map[int64]int64 // clientId -> reqId, provided that one client one rpc at a time
	logIndex2ReqId map[int]int64   // logIndex -> reqId, use to detect leadership change
	conf           shardmaster.Config
	nextReqId      int64
	ownShards      map[int]struct{}
	waitOutShards  map[int]int // shards need to move out-> gid   todo 可能需要保存一个数组 对于出现后一次reConf覆盖前一次还没有完成的情况
	waitInShards   map[int]int // shards need to come in -> gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	defer func() {
		DPrintf("ShardKV(%d-%d).Get args=%v, reply=%v", kv.gid, kv.me, args, reply)
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
		DPrintf("ShardKV(%d-%d).PutAppend args=%v, reply=%v", kv.gid, kv.me, args, reply)
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

// todo check实现
func (kv *ShardKV) MigrateData(args *MigrateDataArgs, reply *MigrateDataReply) {
	defer func() {
		DPrintf("ShardKV(%d-%d).MigrateData args=%v, reply=%v", kv.gid, kv.me, args, reply)
	}()

	reply.Err, _ = kv.start(Op{
		OpType:   OpTypeMigrateData,
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		ConfNum:  args.ConfNum,
		Data:     args.Data,
		Shards:   args.Shards,
	})

	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}
}

func (kv *ShardKV) start(op Op) (Err, string) {
	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("ShardKV(%d-%d) non-leader,", kv.gid, kv.me)
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

	DPrintf("ShardKV(%d-%d) waiting ApplyMsg logIndex(%d), Op=%v", kv.gid, kv.me, logIndex, op)

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

func (kv *ShardKV) applyMessage() {
	var applyMsg raft.ApplyMsg
	for {
		select {
		case applyMsg = <-kv.applyCh:
			kv.mu.Lock()

			if applyMsg.UseSnapshot {
				// todo check snapshot参数
				// handle snapshot 返回的snapshot的data会包含raft log的info
				var includedIndex, includedTerm int
				dec := labgob.NewDecoder(bytes.NewBuffer(applyMsg.Snapshot))
				kv.kvStore = make(map[string]string)
				kv.reqIdCache = make(map[int64]int64)
				kv.logIndex2ReqId = make(map[int]int64)
				kv.conf = shardmaster.Config{}
				if dec.Decode(&includedIndex) != nil ||
					dec.Decode(&includedTerm) != nil ||
					dec.Decode(&kv.kvStore) != nil ||
					dec.Decode(&kv.reqIdCache) != nil ||
					dec.Decode(&kv.logIndex2ReqId) != nil ||
					dec.Decode(&kv.conf) != nil ||
					dec.Decode(&kv.nextReqId) != nil ||
					dec.Decode(&kv.ownShards) != nil ||
					dec.Decode(&kv.waitOutShards) != nil ||
					dec.Decode(&kv.waitInShards) != nil {
					panic("god decode error")
				}
				// todo need to replay logs

				DPrintf("ShardKV(%d-%d) applyMessage includedIndex(%d) includedTerm(%d) restore snapshot", kv.gid, kv.me, includedIndex, includedTerm)
				// todo Should the server update the client state if it returns ErrWrongGroup when executing a Get/Put request?
			} else {
				cmd := applyMsg.Command.(Op)

				notifyMsg := NotifyApplyMsg{err: OK, value: ""}

				// apply log message and deduplicate
				// 对于处于waitIn状态的shard, 这里不返回ErrWrongGroup 让client重试直到数据迁移完成
				if kv.reqIdCache[cmd.ClientId] < cmd.ReqId {
					// 1. put operation
					if cmd.OpType == OpTypePut {
						sh := key2shard(cmd.Key)
						if _, ok := kv.ownShards[sh]; ok {
							kv.kvStore[cmd.Key] = cmd.Value
						} else {
							if _, ok := kv.waitInShards[sh]; ok {
								notifyMsg.err = ErrWaitNewData
							} else {
								notifyMsg.err = ErrWrongGroup
							}
						}
						DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) PutOp key(%v) value(%v), notifyMsg=%v",
							kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, cmd.Key, cmd.Value, notifyMsg)
						// 2. append operation
					} else if cmd.OpType == OpTypeAppend {
						sh := key2shard(cmd.Key)
						if _, ok := kv.ownShards[sh]; ok {
							if v, o := kv.kvStore[cmd.Key]; o {
								kv.kvStore[cmd.Key] = v + cmd.Value
							} else {
								kv.kvStore[cmd.Key] = cmd.Value
							}
						} else {
							if _, ok := kv.waitInShards[sh]; ok {
								notifyMsg.err = ErrWaitNewData
							} else {
								notifyMsg.err = ErrWrongGroup
							}
						}
						DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) PutAndAppendOp key(%v) value(%v), notifyMsg=%v",
							kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, cmd.Key, cmd.Value, notifyMsg)
					}

					// 对于ErrWaitNewData和ErrWrongGroup 这种错误不能更新client的状态 因为同一个reqId的请求还会过来
					if notifyMsg.err != ErrWaitNewData && notifyMsg.err != ErrWrongGroup {
						kv.reqIdCache[cmd.ClientId] = cmd.ReqId
					}
				}

				// 3. get operation
				if cmd.OpType == OpTypeGet {
					sh := key2shard(cmd.Key)
					if _, ok := kv.ownShards[sh]; ok {
						if v, o := kv.kvStore[cmd.Key]; o {
							notifyMsg.value = v
						} else {
							notifyMsg.err = ErrNoKey
						}
					} else {
						if _, ok := kv.waitInShards[sh]; ok {
							notifyMsg.err = ErrWaitNewData
						} else {
							notifyMsg.err = ErrWrongGroup
						}
					}
					DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) GetOp shard(%d) key(%v), notifyMsg=%v",
						kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, sh, cmd.Key, notifyMsg)
				}

				// 4. conf update
				if cmd.OpType == OpTypeConfUpdate {
					if kv.conf.Num < cmd.Conf.Num {
						outGid2Shards := make(map[int][]int) // gid -> shards
						for i := 0; i < shardmaster.NShards; i++ {
							if kv.conf.Shards[i] != cmd.Conf.Shards[i] {
								// 这是要转移出去的分片 并且如果之后该分片的gid为0 表明废弃使用 不用转移数据 todo 何时删除数据
								if kv.conf.Shards[i] == kv.gid {
									kv.waitOutShards[i] = cmd.Conf.Shards[i]
									delete(kv.ownShards, i)
									outGid2Shards[cmd.Conf.Shards[i]] = append(outGid2Shards[cmd.Conf.Shards[i]], i)
								}
								// 这些是要新接受的分片 并且如果之前的该分片的gid为0 不用加入waitingIn直接加入ownShards
								if cmd.Conf.Shards[i] == kv.gid {
									if kv.conf.Shards[i] == 0 {
										kv.ownShards[i] = struct{}{}
									} else {
										kv.waitInShards[i] = kv.conf.Shards[i]
									}
								}
							}
						}
						if _, isLeader := kv.rf.GetState(); isLeader {
							for gi, sh := range outGid2Shards {
								go kv.sendMigrateData(gi, sh)
							}
						} else {
							DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) ConfUpdateOp NonLeader, no need to migrateData",
								kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index)
						}

						DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) ConfUpdateOp, "+
							"kvConf=%v, confArg=%v, outGid2Shards=%v, waitInShards=%v, waitOutShards=%v, ownShards=%v",
							kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, kv.conf, cmd.Conf, outGid2Shards,
							kv.waitInShards, kv.waitOutShards, kv.ownShards)

						// update conf
						kv.conf = kv.makeCopy(cmd.Conf)

					} else {
						DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) ConfUpdateOp useless conf=%v",
							kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, cmd.Conf)
					}
				}

				// 5. migrateData operation
				if cmd.OpType == OpTypeMigrateData {
					// 接收到其他server发来的数据
					// todo 可能的问题 migratedata先执行 然后本地才执行confUpdate 会把之前migrate所修改的ownshard误删
					// todo == 还是 <=
					if cmd.ConfNum <= kv.conf.Num {
						for _, v := range cmd.Shards {
							delete(kv.waitInShards, v)
							kv.ownShards[v] = struct{}{}
						}
						for k, v := range cmd.Data {
							kv.kvStore[k] = v
						}
					} else {
						notifyMsg.err = ErrUnequalNum
					}

					DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) MigrateDataOp shards=%v, notifyMsg=%v",
						kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, cmd.Shards, notifyMsg)
				}

				// 6. return rpc request
				if v, ok := kv.pendingReq[applyMsg.Index]; ok {
					if kv.logIndex2ReqId[applyMsg.Index] == cmd.ReqId {
						v <- notifyMsg
					} else {
						notifyMsg.err = ErrWrongLeader
						notifyMsg.value = ""
						v <- notifyMsg
					}
					delete(kv.logIndex2ReqId, applyMsg.Index)
					delete(kv.pendingReq, applyMsg.Index)
				}

				// 7. snapshot if size more than a fixed size
				if kv.maxRaftState > 0 && kv.persister.RaftStateSize() > kv.maxRaftState {
					var b bytes.Buffer
					enc := labgob.NewEncoder(&b)
					// todo check snapshot参数
					enc.Encode(kv.kvStore)
					enc.Encode(kv.reqIdCache)
					enc.Encode(kv.logIndex2ReqId)
					enc.Encode(kv.conf)
					enc.Encode(kv.nextReqId)
					enc.Encode(kv.ownShards)
					enc.Encode(kv.waitOutShards)
					enc.Encode(kv.waitInShards)
					// 这里一定要用goroutine 否则会产生死锁
					// 死锁条件：applyMsg生产时会先获取锁, 消费时又去获取锁，导致chan不能被消费，从而导致生产被阻塞
					go kv.rf.StartSnapshot(b.Bytes(), applyMsg.Index)
					DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) reach maxRaftState, need snapshot",
						kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index)
				}

				DPrintf("ShardKV(%d-%d) applyMessage finish clientId(%v) reqId(%v) logIndex(%d) applyMsg=%v",
					kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, applyMsg)
			}
			kv.mu.Unlock()
		case <-kv.exitCh:
			return
		}
	}
}

// todo 两个问题
// todo 1. 当需要moveout的数据还在waitIn的时候 需要等待
// todo 2. 不同confupdate 后面一个会把前面一个的waitIn和waitOut覆盖
func (kv *ShardKV) sendMigrateData(gid int, shards []int) {
	kv.mu.Lock()
	servers := kv.conf.Groups[gid]
	if len(servers) == 0 {
		panic("invalid GID")
	}

	args := MigrateDataArgs{
		ClientId: int64(kv.gid),
		ReqId:    kv.nextReqId,
		ConfNum:  kv.conf.Num,
		Shards:   shards,
		Data:     map[string]string{},
	}
	for k, v := range kv.kvStore {
		sh := key2shard(k)
		for _, s := range shards {
			if sh == s {
				args.Data[k] = v
			}
		}
	}
	kv.nextReqId ++
	kv.mu.Unlock()

	defer func() {
		// todo 迁移完成后删除本地数据
		kv.mu.Lock()
		//for k := range args.Data {
		//	delete(kv.kvStore, k)
		//}
		for _, v := range shards {
			delete(kv.waitOutShards, v)
		}
		kv.mu.Unlock()
	}()

	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply MigrateDataReply
			ok := srv.Call("ShardKV.MigrateData", &args, &reply)
			if ok && !reply.WrongLeader && reply.Err == OK {
				DPrintf("ShardKV(%d-%d) sendMigrateData success args=%v, reply=%v", kv.gid, kv.me, args, reply)
				return
			}
			DPrintf("ShardKV(%d-%d) sendMigrateData fail and retry ok=%v, err=%v, args=%v, reply=%v", kv.gid, kv.me, ok, reply.Err, args, reply)
		}
	}
}

func (kv *ShardKV) pollConf() {
	pullTimer := time.NewTicker(time.Duration(60 * time.Millisecond))
	//pollTimer := time.NewTimer(time.Duration(60 * time.Millisecond))
	for {
		kv.mu.Lock()
		_, isLeader := kv.rf.GetState()
		nextConfNum := kv.conf.Num + 1
		kv.mu.Unlock()

		if isLeader {
			newConf := kv.mck.Query(nextConfNum)
			if newConf.Num == nextConfNum {

				kv.mu.Lock()
				reqId := kv.nextReqId
				kv.nextReqId ++
				kv.mu.Unlock()

				// todo update conf
				op := Op{
					OpType:   OpTypeConfUpdate,
					ClientId: -1, // todo updateConf的clientId
					ReqId:    reqId,
					Conf:     kv.makeCopy(newConf),
				}
				DPrintf("ShardKV(%d-%d) poll newConfOp=%v ", kv.gid, kv.me, op)
				if err, _ := kv.start(op); err == OK {
					DPrintf("ShardKV(%d-%d) update conf success, op=%v ", kv.gid, kv.me, op)
				}
			}
		}

		<-pullTimer.C
	}
}

func (kv *ShardKV) makeCopy(source shardmaster.Config) shardmaster.Config {
	res := shardmaster.Config{
		Num:    source.Num,
		Groups: make(map[int][]string),
		Shards: [shardmaster.NShards]int{},
	}

	for k, v := range source.Groups {
		res.Groups[k] = v
	}
	for k, v := range source.Shards {
		res.Shards[k] = v
	}
	return res
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
	DPrintf("ShardKV(%d-%d) exit", kv.gid, kv.me)
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
	labgob.Register(shardmaster.Config{})

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
	kv.nextReqId = nrand() % 10000000001
	kv.ownShards = make(map[int]struct{})
	kv.waitOutShards = make(map[int]int)
	kv.waitInShards = make(map[int]int)

	// todo init conf
	//kv.conf = shardmaster.Config{}
	kv.conf = kv.mck.Query(-1)
	for i, v := range kv.conf.Shards {
		if v == kv.gid {
			kv.ownShards[i] = struct{}{}
		}
	}

	DPrintf("ShardKV(%d-%d) init conf=%v, ownShards=%v ", kv.gid, kv.me, kv.conf, kv.ownShards)

	go kv.applyMessage()
	go kv.pollConf()

	return kv
}
