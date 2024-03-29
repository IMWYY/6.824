package raft

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

type RequestVoteArgs struct {
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogTerm  int //term of candidate’s last log entry
	LastLogIndex int //index of candidate’s last log entry (§5.4)
}

type RequestVoteReply struct {
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 0. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = VoteNull
	}

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	reply.Term = rf.currentTerm
	term := rf.getLastTerm()
	uptoDate := false
	if args.LastLogTerm > term || (args.LastLogTerm == term && args.LastLogIndex >= rf.getLastIndex()) {
		uptoDate = true
	}
	if (rf.votedFor == VoteNull || rf.votedFor == args.CandidateId) && uptoDate {
		rf.chanGrantVote <- struct{}{}
		rf.state = Follower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogTerm  int        // term of prevLogIndex entry
	PrevLogIndex int        // index of log entry immediately preceding new ones
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term      int  // currentTerm, for leader to update itself
	Success   bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	NextIndex int  // back to nextIndex. accelerated log backtracking
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}

	rf.chanHeartbeat <- struct{}{}
	// 0. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// rf.currentTerm has been updated. so here can assign term
	reply.Term = rf.currentTerm

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getLastIndex() {
		reply.NextIndex = rf.getLastIndex() + 1
		return
	}
	// accelerated log backtracking
	baseIndex := rf.log[0].LogIndex
	if args.PrevLogIndex > baseIndex {
		prevTerm := rf.log[args.PrevLogIndex-baseIndex].LogTerm
		if args.PrevLogTerm != prevTerm {
			for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
				if rf.log[i-baseIndex].LogTerm != prevTerm {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// 	  delete the existing entry and all that follow it
	// 4. Append any new entries not already in the log
	if args.PrevLogIndex >= baseIndex {
		rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextIndex = rf.getLastIndex() + 1
	} else {
		// todo handle baseIndex
		// it is correct for the recipient to ignore an InstallSnapshot if the
		// recipient is already ahead of that snapshot. This case can arise in Lab 3
		// for example if the RPC system delivers RPCs out of order.
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = intMin(args.LeaderCommit, rf.getLastIndex())
		rf.chanCommit <- struct{}{}
	}

	return
}

type InstallSnapshotArgs struct {
	Term              int    //leader’s term
	LeaderId          int    //so follower can redirect clients
	LastIncludedIndex int    //the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    //term of lastIncludedIndex
	Data              []byte //raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int //currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.chanHeartbeat <- struct{}{}
	rf.state = Follower

	// save raft state and snapshot
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
	rf.log = truncateLog(args.LastIncludedIndex, args.LastIncludedTerm, rf.log)
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.chanApply <- ApplyMsg{
		UseSnapshot: true,
		Snapshot:    args.Data,
	}
}

// 只保存 lastIncludedIndex 和 lastIncludedTerm 匹配的日志之后的日志
// 这里用log[0]保存 lastIncludedIndex 和 lastIncludedTerm
func truncateLog(lastIncludedIndex int, lastIncludedTerm int, logs []LogEntry) []LogEntry {
	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{
		LogIndex: lastIncludedIndex,
		LogTerm:  lastIncludedTerm,
	})

	for index := len(logs) - 1; index >= 0; index-- {
		if logs[index].LogIndex == lastIncludedIndex && logs[index].LogTerm == lastIncludedTerm {
			newLogEntries = append(newLogEntries, logs[index+1:]...)
			break
		}
	}
	//baseIndex := logs[0].LogIndex
	//for i := lastIncludedIndex + 1; i <= logs[len(logs)-1].LogIndex; i++ {
	//	newLogEntries = append(newLogEntries, logs[i-baseIndex])
	//}
	return newLogEntries
}
