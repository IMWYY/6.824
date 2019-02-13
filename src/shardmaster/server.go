package shardmaster

import (
	"github.com/luci/go-render/render"
	"labgob"
	"labrpc"
	"raft"
	"sort"
	"sync"
	"time"
)

const RequestTimeOut = 1 * time.Second

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs        []Config // indexed by config num
	exitCh         chan struct{}
	pendingReq     map[int]chan NotifyApplyMsg // logIndex -> channel
	reqIdCache     map[int64]int64             // clientId -> reqId, provided that one client one rpc at a time
	logIndex2ReqId map[int]int64               // logIndex -> reqId, use to detect leadership change
}

type NotifyApplyMsg struct {
	err Err
}

type Op struct {
	ReqId    int64
	ClientId int64
	OpType   string
	Args     interface{}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err = sm.start(args.ClientId, args.ReqId, OpTypeJoin, *args)
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err = sm.start(args.ClientId, args.ReqId, OpTypeLeave, *args)
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err = sm.start(args.ClientId, args.ReqId, OpTypeMove, *args)
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.Err = sm.start(args.ClientId, args.ReqId, OpTypeQuery, *args)
	if reply.Err == ErrWrongLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
}

func (sm *ShardMaster) start(clientId, reqId int64, opType string, args interface{}) Err {
	op := Op{
		ReqId:    reqId,
		ClientId: clientId,
		OpType:   opType,
		Args:     args,
	}
	logIndex, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		DPrintf("ShardMaster(%d) non-leader,", sm.me)
		return ErrWrongLeader
	}

	done := make(chan NotifyApplyMsg, 1)
	sm.mu.Lock()
	if _, ok := sm.pendingReq[logIndex]; ok {
		sm.mu.Unlock()
		return ErrWrongLeader
	} else {
		sm.pendingReq[logIndex] = done
	}
	sm.logIndex2ReqId[logIndex] = op.ReqId
	sm.mu.Unlock()

	DPrintf("ShardMaster(%d) waiting ApplyMsg logIndex(%d), Op=%v", sm.me, logIndex, render.Render(op))

	var msg NotifyApplyMsg
	select {
	case msg = <-done:
		return msg.err
	case <-time.After(RequestTimeOut):
		return ErrTimeout
	case <-sm.exitCh:
		return ErrCrash
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	close(sm.exitCh)
	DPrintf("ShardMaster(%d) exit", sm.me)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) run() {
	var applyMsg raft.ApplyMsg
	for {
		select {
		case applyMsg = <-sm.applyCh:
			sm.mu.Lock()

			if applyMsg.UseSnapshot {
				DPrintf("ShardMaster(%d) restore snapshot", sm.me)
			} else {
				cmd,ok := applyMsg.Command.(Op)
				if !ok {
					DPrintf("ShardMaster(%d) receive WRONG message=%v", sm.me, applyMsg)
				}

				// 1. apply log message and deduplicate
				if sm.reqIdCache[cmd.ClientId] < cmd.ReqId {

					if cmd.OpType == OpTypeJoin {
						args := cmd.Args.(JoinArgs)
						newConf := sm.nextNewConfig()
						for k, v := range args.Servers {
							newConf.Groups[k] = v
						}
						sm.rebalanceConfig(newConf)
					} else if cmd.OpType == OpTypeLeave {
						args := cmd.Args.(LeaveArgs)
						newConf := sm.nextNewConfig()
						for _, v := range args.GIDs {
							delete(newConf.Groups, v)
							for i := 0; i < len(newConf.Shards); i++ {
								if newConf.Shards[i] == v {
									newConf.Shards[i] = 0
								}
							}
						}
						sm.rebalanceConfig(newConf)
					} else if cmd.OpType == OpTypeMove {
						args := cmd.Args.(MoveArgs)
						newConf := sm.nextNewConfig()
						if _, ok := newConf.Groups[args.GID]; !ok || args.Shard < 0 || args.Shard >= NShards {
							panic("invalid MoveArgs")
						}
						newConf.Shards[args.Shard] = args.GID
						sm.configs = append(sm.configs, *newConf)
					}
					sm.reqIdCache[cmd.ClientId] = cmd.ReqId
				}

				// 2. return rpc request
				// to detect that it has lost leadership,
				// 	a. by noticing that a different request has appeared at the index returned by Start(),
				// 	b. or that Raft's term has changed
				if v, ok := sm.pendingReq[applyMsg.Index]; ok {
					if sm.logIndex2ReqId[applyMsg.Index] == cmd.ReqId {
						v <- NotifyApplyMsg{err: OK}
					} else {
						v <- NotifyApplyMsg{err: ErrWrongLeader}
					}
					delete(sm.logIndex2ReqId, applyMsg.Index)
					delete(sm.pendingReq, applyMsg.Index)
				}
				DPrintf("ShardMaster(%d) logIndex(%d) receive applyMsg(%v)", sm.me, applyMsg.Index, render.Render(applyMsg))
			}
			sm.mu.Unlock()
		case <-sm.exitCh:
			return
		}
	}

}

type Pair struct {
	gid    int
	shards []int
}

func sortMap(m map[int][]int, desc bool) []Pair {
	if len(m) == 0 {
		return []Pair{}
	}
	pairs := make([]Pair, len(m))
	i := 0
	for k, v := range m {
		pairs[i] = Pair{k, v}
		i ++
	}
	sort.Slice(pairs, func(i, j int) bool {
		if desc {
			return len(pairs[i].shards) > len(pairs[j].shards)
		} else {
			return len(pairs[i].shards) < len(pairs[j].shards)
		}
	})
	return pairs
}

func (sm *ShardMaster) rebalanceConfig(newConf *Config) {
	defer func() {
		sm.configs = append(sm.configs, *newConf)
	}()

	if len(newConf.Groups) == 0 {
		return
	}

	// gid->shardsIndex and sort
	gid2shardsIndex := map[int][]int{}
	// 还没有被分配的shards
	waitAllocShards := Pair{
		gid:    0,
		shards: []int{},
	}
	// 所有还没被分配过的group对应的shards都会被初始化为0
	for k := range newConf.Groups {
		gid2shardsIndex[k] = []int{}
	}
	for k, v := range newConf.Shards {
		// gid=0 该shard还没被分配 要优先分配
		if v == 0 {
			waitAllocShards.shards = append(waitAllocShards.shards, k)
			continue
		}
		if _, ok := gid2shardsIndex[v]; ok {
			gid2shardsIndex[v] = append(gid2shardsIndex[v], k)
		} else {
			gid2shardsIndex[v] = []int{k}
		}
	}
	ascPairs := sortMap(gid2shardsIndex, false)

	min := NShards / len(newConf.Groups)
	max := min + 1
	if min == 0 || len(newConf.Groups) == NShards {
		min, max = 1, 1
	}

	DPrintf("ShardMaster(%d) min(%d), max(%d), ascPairs=%v waitAllocShards=%v", sm.me, min, max, ascPairs, waitAllocShards)

	// after leave operation
	index := 0
	for len(waitAllocShards.shards) > 0 {
		for _, v := range ascPairs {
			if len(v.shards) < min {
				for len(v.shards) < min {
					v.shards = append(v.shards, waitAllocShards.shards[index])
					newConf.Shards[waitAllocShards.shards[index]] = v.gid
					index ++
					if index == len(waitAllocShards.shards) {
						return
					}
				}
			} else if len(v.shards) < max {
				v.shards = append(v.shards, waitAllocShards.shards[index])
				newConf.Shards[waitAllocShards.shards[index]] = v.gid
				index ++
				if index == len(waitAllocShards.shards) {
					return
				}
			}
		}
	}

	// after add operation
	low, high := 0, len(ascPairs)-1
	for {
		small := ascPairs[low]
		if len(small.shards) >= min || low == high {
			return
		}
		big := ascPairs[high]
		// 把分配的shards数量高于avg的 分配给低于avg的group
		for i := len(big.shards) - 1; i >= min; i-- {
			small.shards = append(small.shards, big.shards[i])
			newConf.Shards[big.shards[i]] = small.gid
			big.shards = big.shards[:i]
			if len(small.shards) == min {
				low ++
				if low == high {
					return
				}
				small = ascPairs[low]
				if len(small.shards) >= min {
					return
				}
			}
		}
		high --
	}
}

func (sm *ShardMaster) nextNewConfig() *Config {
	oldConf := sm.configs[len(sm.configs)-1]
	newConf := &Config{}
	newConf.Num = len(sm.configs)
	newConf.Groups = make(map[int][]string)
	newConf.Shards = [NShards]int{}
	for k, v := range oldConf.Groups {
		newConf.Groups[k] = v
	}
	for k, v := range oldConf.Shards {
		newConf.Shards[k] = v
	}
	return newConf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.exitCh = make(chan struct{})
	sm.reqIdCache = make(map[int64]int64)
	sm.logIndex2ReqId = make(map[int]int64)
	sm.pendingReq = make(map[int]chan NotifyApplyMsg)

	go sm.run()

	return sm
}
