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

// TODO 遇到的问题与解决方法
// 1. 数据迁移在不同时刻不是幂等的，不能重复执行，当实例重启后 依据snapshot的conf进行数据迁移，
//   可能会导致之前已经迁移过的数据又迁移了一边, 把其他实例上可能已经被更新过的数据给覆盖了
//   可以通过在接受迁移的数据时，判断如果waitInShards存在且对应的confNum与参数相等，才会去覆盖本地数据；否则认为这个请求已经处理过了
// 2. 一次必须只能apply一个reConf，否则会出现上一个conf的数据还没有迁移完成，下一个reConf开始，导致还没有拿到需要迁移的数据
//   可以通过在刷新conf的时候通过判断waitInShards的长度来判断是否还有数据没有迁移完，如果没有则说明上一个reConf已经处理完
// 3. 对于ErrWaitNewData和ErrWrongGroup 这种错误不能更新server端client的reqId状态 因为同一个reqId的请求还会过来
// 4. 刷新conf和删除数据只能从leader开始，否则会有raft之间状态不一致
// 5. 迁移数据也需要将reqCache迁移 保证同样的请求不会在不同的group重复执行 并且在更新ReqCache的时候需要与本地的ReqCache比较 选取大的那个
// 6. 当向外发送迁移的数据时，如果此时crash，在restart的时候需要继续将之前的数据send出去

const RequestTimeOut = 1 * time.Second

type Op struct {
	// Common
	OpType   string
	ClientId int64
	ReqId    int64

	// PutAppend & Get
	Key   string
	Value string

	// Conf Update
	Conf shardmaster.Config

	// Migrate Data
	ConfNum  int
	ToGid    int
	Data     map[string]string
	ReqCache map[int64]int64
	Shards   []int
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
	waitInShards   map[int]int                 // shards need to come in -> conf Num
	waitOutShards  map[int]map[int]MigrateData // conf Num -> shards data need to move out
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

func (kv *ShardKV) MigrateData(args *MigrateDataArgs, reply *MigrateDataReply) {
	defer func() {
		DPrintf("ShardKV(%d-%d).MigrateData args=%v, reply=%v", kv.gid, kv.me, args, reply)
	}()

	reply.Err, _ = kv.start(Op{
		OpType:   OpTypeMigrateData,
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
		ConfNum:  args.ConfNum,
		ToGid:    args.ToGid,
		Data:     args.Data,
		ReqCache: args.ReqCache,
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
		kv.mu.Lock()
		delete(kv.pendingReq, logIndex)
		kv.mu.Unlock()
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
				// handle snapshot 返回的snapshot的data会包含raft log的info
				var includedIndex, includedTerm int
				dec := labgob.NewDecoder(bytes.NewBuffer(applyMsg.Snapshot))
				kv.kvStore = make(map[string]string)
				kv.reqIdCache = make(map[int64]int64)
				kv.logIndex2ReqId = make(map[int]int64)
				kv.ownShards = make(map[int]struct{})
				kv.waitInShards = make(map[int]int)
				kv.conf = shardmaster.Config{}
				if dec.Decode(&includedIndex) != nil ||
					dec.Decode(&includedTerm) != nil ||
					dec.Decode(&kv.kvStore) != nil ||
					dec.Decode(&kv.reqIdCache) != nil ||
					dec.Decode(&kv.logIndex2ReqId) != nil ||
					dec.Decode(&kv.conf) != nil ||
					dec.Decode(&kv.nextReqId) != nil ||
					dec.Decode(&kv.ownShards) != nil ||
					dec.Decode(&kv.waitInShards) != nil ||
					dec.Decode(&kv.waitOutShards) != nil {
					panic("god decode error")
				}

				// todo Challenge1 如果有还没有迁移完的数据 继续迁移
				// for num := range kv.waitOutShards {
				//	if num >= kv.conf.Num {
				//		go kv.startMigrateData(num)
				//	}
				//}

				DPrintf("ShardKV(%d-%d) applyMessage includedIndex(%d) includedTerm(%d) restore snapshot", kv.gid, kv.me, includedIndex, includedTerm)
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
						DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) PutAndAppendOp key(%v) appendValue(%v), value(%v), notifyMsg=%v",
							kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, cmd.Key, cmd.Value, kv.kvStore[cmd.Key], notifyMsg)
					}

					// 对于ErrWaitNewData和ErrWrongGroup 这种错误不能更新client的状态 因为同一个reqId的请求还会过来
					if notifyMsg.err != ErrWaitNewData && notifyMsg.err != ErrWrongGroup {
						kv.reqIdCache[cmd.ClientId] = cmd.ReqId
					}
				}

				// 3. get请求 只读请求不需要检查reqIdCache
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

				// 4. conf更新的条件是 新的confNum比当前的大
				if cmd.OpType == OpTypeConfUpdate {
					if kv.conf.Num < cmd.Conf.Num {
						outGid2Shards := make(map[int][]int) // gid -> shards
						for i := 0; i < shardmaster.NShards; i++ {
							if kv.conf.Shards[i] != cmd.Conf.Shards[i] {
								// 这是要转移出去的分片 并且如果之后该分片的gid为0 表明废弃使用 不用转移数据
								if kv.conf.Shards[i] == kv.gid {
									delete(kv.ownShards, i)
									outGid2Shards[cmd.Conf.Shards[i]] = append(outGid2Shards[cmd.Conf.Shards[i]], i)
								}
								// 这些是要新接受的分片 并且如果之前的该分片的gid为0 不用加入waitingIn直接加入ownShards
								if cmd.Conf.Shards[i] == kv.gid {
									if kv.conf.Shards[i] == 0 {
										kv.ownShards[i] = struct{}{}
									} else {
										kv.waitInShards[i] = cmd.Conf.Num
									}
								}
							}
						}
						if _, isLeader := kv.rf.GetState(); isLeader && len(outGid2Shards) > 0 {
							for gi, shards := range outGid2Shards {
								args := MigrateDataArgs{
									ClientId: int64(kv.gid),
									ReqId:    kv.nextReqId,
									ConfNum:  cmd.Conf.Num,
									ToGid:    gi,
									Shards:   shards,
									Data:     map[string]string{},
									ReqCache: map[int64]int64{},
								}
								for k, v := range kv.kvStore {
									sh := key2shard(k)
									for _, s := range shards {
										if sh == s {
											args.Data[k] = v
											// 这里就可以将要转移的数据从本地删除
											delete(kv.kvStore, k)
										}
									}
								}
								for k, v := range kv.reqIdCache {
									args.ReqCache[k] = v
								}
								//if _, ex := kv.waitOutShards[cmd.Conf.Num]; !ex {
								//	kv.waitOutShards[cmd.Conf.Num] = make(map[int]MigrateData)
								//}
								//kv.waitOutShards[cmd.Conf.Num][gi] = MigrateData{
								//	Servers:  cmd.Conf.Groups[gi],
								//	DataArgs: args,
								//}
								kv.nextReqId ++
								go kv.sendMigrateData(cmd.Conf.Groups[gi], &args)
							}
							//go kv.startMigrateData(cmd.Conf.Num)
						} else {
							DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) ConfUpdateOp NonLeader, no need to migrateData",
								kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index)
						}

						DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) ConfUpdateOp, "+
							"kvConf=%v, confArg=%v, outGid2Shards=%v, waitInShards=%v, ownShards=%v",
							kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, kv.conf, cmd.Conf, outGid2Shards,
							kv.waitInShards, kv.ownShards)

						// remember to update conf
						kv.conf = cmd.Conf.Copy()

					} else {
						DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) ConfUpdateOp useless conf=%v",
							kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, cmd.Conf)
					}
				}

				// 5. 接受迁移数据 同时需要保证confNum对应才能删除本地的waitInShard
				if cmd.OpType == OpTypeMigrateData {
					if cmd.ConfNum <= kv.conf.Num {
						for k, v := range cmd.Data {
							if n, ok := kv.waitInShards[key2shard(k)]; ok && n == cmd.ConfNum {
								kv.kvStore[k] = v
							}
						}
						for k, v := range cmd.ReqCache {
							if kv.reqIdCache[k] < v { // 这里必须要有这个条件 否则已经提交的请求的id会被覆盖
								kv.reqIdCache[k] = v
							}
						}
						for _, v := range cmd.Shards {
							if nu, ex := kv.waitInShards[v]; ex && nu == cmd.ConfNum {
								delete(kv.waitInShards, v)
								kv.ownShards[v] = struct{}{}
							}
						}
					} else {
						notifyMsg.err = ErrUnequalNum
					}

					DPrintf("ShardKV(%d-%d) applyMessage clientId(%v) reqId(%v) logIndex(%d) MigrateDataOp shards=%v, notifyMsg=%v",
						kv.gid, kv.me, cmd.ClientId, cmd.ReqId, applyMsg.Index, cmd.Shards, notifyMsg)
				}

				// 6. 返回RPC请求
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
					enc.Encode(kv.kvStore)
					enc.Encode(kv.reqIdCache)
					enc.Encode(kv.logIndex2ReqId)
					enc.Encode(kv.conf)
					enc.Encode(kv.nextReqId)
					enc.Encode(kv.ownShards)
					enc.Encode(kv.waitInShards)
					enc.Encode(kv.waitOutShards)
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

func (kv *ShardKV) startMigrateData(confNum int) {
	DPrintf("ShardKV(%d-%d) start startMigrateData confNum=%v", kv.gid, kv.me, confNum)
	defer func() {
		DPrintf("ShardKV(%d-%d) finish startMigrateData confNum=%v", kv.gid, kv.me, confNum)
	}()
	wg := sync.WaitGroup{}
	kv.mu.Lock()
	wg.Add(len(kv.waitOutShards[confNum]))
	for _, args := range kv.waitOutShards[confNum] {
		go func() {
			kv.sendMigrateData(args.Servers, &args.DataArgs)
			wg.Done()
		}()
	}
	kv.mu.Unlock()

	wg.Wait()

	kv.mu.Lock()
	delete(kv.waitOutShards, confNum)
	kv.mu.Unlock()
}

func (kv *ShardKV) sendMigrateData(servers []string, args *MigrateDataArgs) {
	if len(servers) == 0 {
		panic("invalid GID")
	}

	DPrintf("ShardKV(%d-%d)-Start sendMigrateData args=%v", kv.gid, kv.me, args)

	for {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply MigrateDataReply
			ok := srv.Call("ShardKV.MigrateData", args, &reply)
			if ok && !reply.WrongLeader && reply.Err == OK {
				DPrintf("ShardKV(%d-%d)-SendMigrateData success args=%v, reply=%v", kv.gid, kv.me, args, reply)
				return
			}
			DPrintf("ShardKV(%d-%d)-SendMigrateData fail and retry ok=%v, err=%v, args=%v, reply=%v", kv.gid, kv.me, ok, reply.Err, args, reply)
		}
	}
}

func (kv *ShardKV) pullConf() {
	pullTimer := time.NewTicker(time.Duration(50 * time.Millisecond))
	for {
		kv.mu.Lock()
		// 这里的条件保证一次只有一个config在进行
		if _, isLeader := kv.rf.GetState(); !isLeader || len(kv.waitInShards) != 0 {
			kv.mu.Unlock()
			continue
		}
		nextConfNum := kv.conf.Num + 1
		kv.mu.Unlock()

		newConf := kv.mck.Query(nextConfNum)
		if newConf.Num == nextConfNum {
			kv.mu.Lock()
			reqId := kv.nextReqId
			kv.nextReqId ++
			kv.mu.Unlock()

			op := Op{
				OpType:   OpTypeConfUpdate,
				ClientId: -1,
				ReqId:    reqId,
				Conf:     newConf.Copy(),
			}
			DPrintf("ShardKV(%d-%d) poll newConfOp=%v ", kv.gid, kv.me, op)
			if err, _ := kv.start(op); err == OK {
				DPrintf("ShardKV(%d-%d) update conf success, op=%v ", kv.gid, kv.me, op)
			}
		}

		<-pullTimer.C
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
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(MigrateData{})
	labgob.Register(MigrateDataArgs{})
	labgob.Register(MigrateDataReply{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
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
	kv.waitInShards = make(map[int]int)
	kv.waitOutShards = make(map[int]map[int]MigrateData)

	kv.conf = shardmaster.Config{}

	go kv.applyMessage()
	go kv.pullConf()

	DPrintf("ShardKV(%d-%d) start", kv.gid, kv.me)

	return kv
}
