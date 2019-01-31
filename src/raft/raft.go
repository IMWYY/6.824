package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.

import (
	"github.com/luci/go-render/render"
	"math/rand"
	"sync"
	"time"
)
import (
	"bytes"
	"encoding/gob"
	"labrpc"
)

type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
	Shutdown
)

const (
	VoteNull          = -1
	HeartbeatInterval = 50 * time.Millisecond
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Shutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	state RaftState

	// all servers persistent
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// additional
	voteCount     int
	chanCommit    chan bool
	chanHeartbeat chan bool
	chanGrantVote chan bool
	chanLeader    chan bool
	chanApply     chan ApplyMsg
}

type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat
	// may send more than one for efficiency)
	LeaderCommit int //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	//ConflictTerm  int
	//ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	//Offset            int    // byte offset where chunk is positioned in the snapshot file
	//Done              bool   //true if this is the last chunk
}
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getBaseLogIndex() int {
	return rf.log[0].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// random election timeout 150-300ms
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var LastIncludedIndex int
	var LastIncludedTerm int
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex

	rf.updateLogWithSnapshot(LastIncludedIndex, LastIncludedTerm)

	msg := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    data,
	}

	go func() {
		rf.chanApply <- msg
	}()
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist()
		DPrintf("Server(%d) Term(%d) response to RequestVote, args=%v, reply=%v \n", rf.me, rf.currentTerm, render.Render(args), render.Render(reply))
		rf.mu.Unlock()
	}()

	reply.VoteGranted = false
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	// Raft determines which of two logs is more up-to-date by comparing the
	// index and term of the last entries in the logs. If the logs have last entries
	// with different terms, then the log with the later term is more up-to-date.
	// If the logs end with the same term, then whichever log is longer is more up-to-date
	uptodate := false

	if args.LastLogTerm > rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
		uptodate = true
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == VoteNull || rf.votedFor == args.CandidateId) && uptodate {
		rf.state = Follower
		rf.chanGrantVote <- true
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// receive rpc request from leader. see as heartbeat
	rf.chanHeartbeat <- true

	// todo check whether to put this response checking into Make()
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	baseIndex := rf.getBaseLogIndex() // must ensure len(log) >= 1
	if (args.PrevLogIndex > baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term) ||
		args.PrevLogIndex < baseIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Append any new entries not already in the log
	if len(args.Entries) > 0 {
		rf.log = rf.log[:args.PrevLogIndex+1-baseIndex]
		rf.log = append(rf.log, args.Entries...)
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	//  If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := rf.getLastLogIndex()
		if args.LeaderCommit > lastIndex {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.chanCommit <- true
	}
	return
}

// Save snapshot file, discard any existing or partial snapshot with a smaller index
func (rf *Raft) updateLogWithSnapshot(LastIncludedIndex, LastIncludedTerm int) {
	match := false
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == LastIncludedIndex && rf.log[i].Term == LastIncludedTerm {
			match = true
			rf.log = rf.log[i:]
		}
	}
	// make sure len(log) > 0
	if !match {
		rf.log = []LogEntry{{
			Term:  LastIncludedTerm,
			Index: LastIncludedIndex,
		}}
	}
}
func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		args.Term = rf.currentTerm
		return
	}

	rf.chanHeartbeat <- true
	rf.state = Follower
	rf.persister.SaveSnapshot(args.Data)

	rf.updateLogWithSnapshot(args.LastIncludedIndex, args.LastIncludedTerm)

	msg := ApplyMsg{
		UseSnapshot: true,
		Snapshot:    args.Data,
	}
	// update lastApplied and commitIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	rf.persist()
	rf.chanApply <- msg
}

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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// leader send append entry request to other peers
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	N := rf.commitIndex
	for i := rf.commitIndex + 1; i <= rf.getLastLogIndex(); i++ {
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-rf.getBaseLogIndex()].Term == rf.currentTerm {
				count ++
			}
		}
		if count > len(rf.peers)/2 {
			N = i
		}
	}
	if N > rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- true
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == Leader {
			if rf.nextIndex[i] > rf.getBaseLogIndex() {
				entries := make([]LogEntry, rf.getLastLogIndex()-rf.nextIndex[i]+1)
				copy(entries, rf.log[rf.nextIndex[i]-rf.getBaseLogIndex():])

				logArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1-rf.getBaseLogIndex()].Term,
					LeaderCommit: rf.commitIndex,
					Entries:      entries,
				}

				go func(serverId int, args AppendEntriesArgs) {
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(serverId, args, &reply)

					DPrintf("server(%d) broadcast to server(%d) AppendEntry  args=%v, reply=%v \n", rf.me, serverId, args, reply)

					rf.mu.Lock()
					if ok {
						if reply.Term > rf.currentTerm {
							rf.convertToFollower(reply.Term)
						} else {
							// if successful: update nextIndex and matchIndex for follower
							if reply.Success {
								if len(args.Entries) > 0 {
									rf.nextIndex[serverId] = args.Entries[len(args.Entries)-1].Index + 1
									rf.matchIndex[serverId] = rf.nextIndex[serverId] - 1
								}
							} else {
								// If fails because of log inconsistency: decrement nextIndex and retry
								rf.nextIndex[serverId] --
							}
						}
					}
					rf.mu.Unlock()
				}(i, logArgs)

			} else {
				// handle snapshot
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.getBaseLogIndex(),
					LastIncludedTerm:  rf.log[0].Term,
					Data:              rf.persister.snapshot,
				}

				go func(serverId int) {
					var reply InstallSnapshotReply
					ok := rf.sendInstallSnapshot(serverId, args, &reply)
					if ok {
						if reply.Term > rf.currentTerm {
							rf.convertToFollower(reply.Term)
						} else {
							rf.nextIndex[serverId] = args.LastIncludedIndex + 1
							rf.matchIndex[serverId] = args.LastIncludedIndex
						}
					}
				}(i)
			}
		}
	}
}

// candidate send request vote to all the other peers
func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastLogTerm(),
		LastLogIndex: rf.getLastLogIndex(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me {

			go func(serverId int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(serverId, args, &reply)

				DPrintf("server(%d) broadcast to server(%d) RequestVote args=%v, reply=%v \n", rf.me, serverId, args, reply)

				rf.mu.Lock()
				// rpc success and state not change and in the same term
				if ok && rf.state == Candidate && args.Term == rf.currentTerm {
					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
					} else {
						if reply.VoteGranted {
							rf.voteCount ++
							if rf.voteCount > len(rf.peers)/2 {
								rf.state = Follower
								rf.chanLeader <- true
							}
						}
					}
				}
				rf.mu.Unlock()
			}(i, voteArgs)
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// if is leader append log and persist
	if isLeader {
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Index:   index,
			Command: command,
		})
		rf.persist()
	}

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	DPrintf("server(%v) state(%v) term(%v) been killed \n", rf.me,
		rf.state.String(), rf.currentTerm)
}

// reply to client when finish committing command
func (rf *Raft) replyToClient() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			Index:   i,
			Command: rf.log[i-rf.getBaseLogIndex()].Command,
		}
		rf.chanApply <- msg
		rf.lastApplied = i
	}
}

// 同步调用 不要加锁
func (rf *Raft) convertToFollower(term int) {
	//fmt.Printf("Server(%d) convert To Follower, Term %d \n", rf.me, term)
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = VoteNull
	rf.voteCount = 0
	rf.persist()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = VoteNull
	rf.log = []LogEntry{{Term: 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	rf.voteCount = 1
	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanApply = applyCh

	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// different behavior according to different state
	go func() {
		for {
			switch rf.state {
			case Leader:
				DPrintf("Server(%d) Leader doing staff \n", rf.me)
				// Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries)
				// to all followers in order to maintain their authority
				rf.broadcastAppendEntries()
				time.Sleep(HeartbeatInterval)
			case Follower:
				DPrintf("Server(%d) Follower doing staff \n", rf.me)
				select {
				case <-rf.chanHeartbeat:
				case <-rf.chanGrantVote:
					// If a follower receives no communication over a period of time called the election timeout,
					// then it assumes there is no vi- able leader and begins an election to choose a new leader.
				case <-time.After(rf.getElectionTimeout()):
					rf.state = Candidate
				}
			case Candidate:
				DPrintf("Server(%d) Candidate doing staff \n", rf.me)
				// To begin an election, a follower increments its current term and transitions to candidate state.
				rf.mu.Lock()
				rf.currentTerm ++
				rf.votedFor = me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()

				// It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster
				go rf.broadcastRequestVote()

				// A candidate continues in this state until one of three things happens
				select {
				// (a) it wins the election
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					//  When a leader first comes to power, it initializes all nextIndex values to the index
					// just after the last one in its log
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()

					// (b) another server establishes itself as leader
				case <-rf.chanHeartbeat:
					rf.state = Follower

					// (c) a period of time goes by with no winner
				case <-time.After(rf.getElectionTimeout()):
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.chanCommit:
				rf.replyToClient()
			}
		}
	}()

	return rf
}
