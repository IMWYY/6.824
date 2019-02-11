package raft

//
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
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	Leader = iota
	Candidate
	Follower

	HeartbeatInterval    = 125 * time.Millisecond
	ElectionTimeoutBase  = 300
	ElectionTimeoutDelta = 151
	VoteNull             = -1
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	LogIndex int
	LogTerm  int
	LogCmd   interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	state         int
	voteCount     int
	chanCommit    chan struct{}
	chanHeartbeat chan struct{}
	chanGrantVote chan struct{}
	chanLeader    chan struct{}
	chanApply     chan ApplyMsg
	chanExit      chan struct{}

	//persistent state on all server
	currentTerm int
	votedFor    int
	log         []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(rand.Int63()%ElectionTimeoutDelta+ElectionTimeoutBase) * time.Millisecond
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getPersistData())
}

func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) readSnapshot(data []byte) {
	rf.readPersist(rf.persister.ReadRaftState())

	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var LastIncludedIndex int
	var LastIncludedTerm int
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)

	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex
	rf.log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.log)

	go func() {
		rf.chanApply <- ApplyMsg{
			UseSnapshot: true,
			Snapshot:    data,
		}
	}()
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	DPrintf("Raft(%d) StartSnapshot lock before, index=%v", rf.me, index)
	rf.mu.Lock()
	DPrintf("Raft(%d) StartSnapshot lock enter, index=%v", rf.me, index)
	defer DPrintf("Raft(%d) StartSnapshot lock leave, index=%v", rf.me, index)
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].LogIndex
	lastIndex := rf.getLastIndex()

	if index <= baseIndex || index > lastIndex {
		// in case having installed a snapshot from leader before snapshotting
		// second condition is a hack
		return
	}

	lastIncludedIndex := index
	lastIncludedTerm := rf.log[index-baseIndex].LogTerm

	rf.log = truncateLog(lastIncludedIndex, lastIncludedTerm, rf.log)

	// encode lastIncludedIndex & lastIncludedTerm
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(lastIncludedIndex)
	e.Encode(lastIncludedTerm)
	data := w.Bytes()
	data = append(data, snapshot...)

	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), data)
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("Raft(%d) Start lock before, cmd=%v", rf.me, command)
	rf.mu.Lock()
	DPrintf("Raft(%d) Start lock enter, cmd=%v", rf.me, command)
	defer DPrintf("Raft(%d) Start lock leave, cmd=%v", rf.me, command)
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	if isLeader {
		index = rf.getLastIndex() + 1
		rf.log = append(rf.log, LogEntry{
			LogTerm:  term,
			LogCmd:   command,
			LogIndex: index,
		})
		rf.persist()
		//go rf.broadcastAppendEntries()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.chanExit)
	DPrintf("%v(%v) term(%v) been killed \n", rf.state, rf.me, rf.currentTerm)
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastTerm(),
		LastLogIndex: rf.getLastIndex(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == Candidate {

			go func(index int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(index, voteArgs, &reply)
				DPrintf("Raft(%d) broadcastRequestVote lock before", rf.me)
				rf.mu.Lock()
				DPrintf("Raft(%d) broadcastRequestVote  lock enter", rf.me)
				defer DPrintf("Raft(%d) broadcastRequestVote lock leave", rf.me)
				defer rf.mu.Unlock()
				if ok {
					if rf.state != Candidate || voteArgs.Term != rf.currentTerm {
						return
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = VoteNull
						rf.persist()
					}
					if reply.VoteGranted {
						rf.voteCount++
						if rf.state == Candidate && rf.voteCount > len(rf.peers)/2 {
							rf.state = Follower
							rf.chanLeader <- struct{}{}
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	DPrintf("Raft(%d) broadcastAppendEntries lock before", rf.me)
	rf.mu.Lock()
	DPrintf("Raft(%d) broadcastAppendEntries  lock enter", rf.me)
	defer DPrintf("Raft(%d) broadcastAppendEntries lock leave", rf.me)
	defer rf.mu.Unlock()

	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
	// and log[N].term == currentTerm: set commitIndex = N
	N := rf.commitIndex
	baseIndex := rf.log[0].LogIndex
	for i := rf.commitIndex + 1; i <= rf.getLastIndex(); i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && j < len(rf.matchIndex) && rf.matchIndex[j] >= i &&
				i-baseIndex < len(rf.log) && rf.log[i-baseIndex].LogTerm == rf.currentTerm {
				num++
			}
		}
		if 2*num > len(rf.peers) {
			N = i
		}
	}
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- struct{}{}
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == Leader { // here we need to check the state of raft in case we receive an outdated request

			if rf.nextIndex[i] > baseIndex {
				entries := make([]LogEntry, len(rf.log[rf.nextIndex[i]-baseIndex:]))
				copy(entries, rf.log[rf.nextIndex[i]-baseIndex:])
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1-baseIndex].LogTerm,
					LeaderCommit: rf.commitIndex,
					Entries:      entries,
				}

				go func(index int, req AppendEntriesArgs) {
					var reply AppendEntriesReply
					ok := rf.sendAppendEntries(index, req, &reply)

					DPrintf("Raft(%d) to (%d) sendAppendEntries lock before", rf.me, index)
					rf.mu.Lock()
					DPrintf("Raft(%d) to (%d) sendAppendEntries  lock enter", rf.me, index)
					defer DPrintf("Raft(%d) to (%d) sendAppendEntries lock leave", rf.me, index)
					defer rf.mu.Unlock()
					if ok {
						if rf.state != Leader || req.Term != rf.currentTerm {
							return
						}
						// convert to follower
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = VoteNull
							rf.persist()
							return
						}
						if reply.Success {
							if len(req.Entries) > 0 {
								logIndexToCommit := req.Entries[len(req.Entries)-1].LogIndex
								rf.nextIndex[index] = logIndexToCommit + 1
								rf.matchIndex[index] = rf.nextIndex[index] - 1

								count := 0
								for j := 0; j < len(rf.peers); j++ {
									if rf.matchIndex[j] >= logIndexToCommit {
										count += 1
									}
								}
								if count > len(rf.peers)/2 && rf.commitIndex < logIndexToCommit {
									rf.commitIndex = logIndexToCommit
									rf.chanCommit <- struct{}{}
								}
							}
						} else {
							rf.nextIndex[index] = reply.NextIndex
						}
					}
				}(i, args)
			} else {
				// if follower's log ends before leader's log starts
				// nextIndex[i] will back up to start of leader's log
				// so leader can't repair that follower with AppendEntries RPCs thus the InstallSnapshot RPC
				snapShortArgs := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.log[0].LogIndex, // log[0]保存了lastIncluded xxx
					LastIncludedTerm:  rf.log[0].LogTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				go func(server int, args InstallSnapshotArgs) {
					var reply InstallSnapshotReply
					ok := rf.sendInstallSnapshot(server, args, &reply)
					DPrintf("Raft(%d) to (%d) sendInstallSnapshot lock before", rf.me, server)
					rf.mu.Lock()
					DPrintf("Raft(%d) to (%d) sendInstallSnapshot  lock enter", rf.me, server)
					defer DPrintf("Raft(%d) to (%d) sendInstallSnapshot lock leave", rf.me, server)
					defer rf.mu.Unlock()
					if ok {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = VoteNull
							rf.persist()
							return
						}
						rf.nextIndex[server] = args.LastIncludedIndex + 1
						rf.matchIndex[server] = args.LastIncludedIndex
					}
				}(i, snapShortArgs)
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0
	rf.chanCommit = make(chan struct{}, 100)
	rf.chanHeartbeat = make(chan struct{}, 100)
	rf.chanGrantVote = make(chan struct{}, 100)
	rf.chanLeader = make(chan struct{}, 100)
	rf.chanExit = make(chan struct{})
	rf.chanApply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go func() {
	Loop:
		for {
			select {
			case <-rf.chanExit:
				break Loop
			default:
			}

			switch rf.state {
			case Follower:
				select {
				case <-rf.chanHeartbeat:
				case <-rf.chanGrantVote:
				case <-time.After(rf.getElectionTimeout()):
					rf.state = Candidate
				}
			case Leader:
				rf.broadcastAppendEntries()
				time.Sleep(HeartbeatInterval)
			case Candidate:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				rf.mu.Unlock()

				go rf.broadcastRequestVote()

				select {
				case <-time.After(rf.getElectionTimeout()):
				case <-rf.chanHeartbeat:
					rf.state = Follower
				case <-rf.chanLeader:
					rf.mu.Lock()
					rf.state = Leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.chanCommit:
				DPrintf("Raft(%d) chanCommit lock before", rf.me)
				rf.mu.Lock()
				DPrintf("Raft(%d) chanCommit  lock enter", rf.me)
				baseIndex := rf.log[0].LogIndex
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					applyCh <- ApplyMsg{
						Index:   i,
						Command: rf.log[i-baseIndex].LogCmd,
					}
					rf.lastApplied = i
				}
				rf.mu.Unlock()
				DPrintf("Raft(%d) chanCommit lock leave", rf.me)
			case <-rf.chanExit:
				return
			}
		}
	}()
	return rf
}
