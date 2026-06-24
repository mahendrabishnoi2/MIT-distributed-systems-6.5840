package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type RaftCommand struct {
	Data any
}

type LogEntry struct {
	Term    int
	Command RaftCommand
}

type RaftPeerStae int

const (
	Follower RaftPeerStae = iota
	Candidate
	Leader
)

var _ raftapi.Raft = (*Raft)(nil)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      atomic.Int32        // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    *int
	logs        []LogEntry
	state       RaftPeerStae // starts as follower (0)

	commitIndex int // index of last committed log entry, starts at 0
	lastApplied int // index of highest log entry applied to raft state machine, starts at 0

	// only for leader
	nextIndex  []any
	matchIndex []any

	lastHeartbeatAt time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateID  int
	LastLogTerm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("peer %d: received RequestVote RPC from peer %d, args: %+v", rf.me, args.CandidateID, *args)
	// Your code here (3A, 3B).
	defer func() {
		DPrintf("peer %d: RequestVote RPC result to peer %d: %+v", rf.me, args.CandidateID, *reply)
	}()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateID
		rf.state = Follower
		return
	}

	// same term as vote requetor
	if len(rf.logs) >= args.LastLogIndex {
		if rf.state == Leader {
			return
		}
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateID
	}
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("peer %d: received AppendEntries from peer %d, args: %+v", rf.me, args.LeaderId, *args)
	reply.Term = rf.currentTerm
	reply.Success = false

	// todo: implementation for modifying log, not required for 3A tests
	// 3A tests mainly focus on heartbeats and leader election
	// heartbeats are not saved to log
	if args.Term < rf.currentTerm {
		return
	}
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = nil
	rf.lastHeartbeatAt = time.Now()
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	rf.mu.Lock()
	isLeader := rf.state == Leader
	rf.mu.Unlock()

	if !isLeader {
		return index, term, false
	}

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	rf.dead.Store(1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := rf.dead.Load()
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350 milliseconds.
		ms := 50 + (rand.Int63() % 300)
		electionTimeoutDuration := time.Duration(ms) * time.Millisecond
		time.Sleep(electionTimeoutDuration)

		rf.mu.Lock()
		if rf.state == Leader || time.Since(rf.lastHeartbeatAt) < 500*time.Millisecond { // due to whatever reasons if not in follower state, skip election
			rf.mu.Unlock()
			continue
		}

		// can start leader election now
		rf.state = Candidate
		rf.votedFor = &rf.me
		rf.currentTerm++

		term := rf.currentTerm
		lastLogTerm := 0
		if len(rf.logs) > 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		lastLogIndex := len(rf.logs) - 1
		rf.mu.Unlock()

		var votesReceived atomic.Int32
		votesReceived.Add(1) // self vote

		// send request vote RPCs to all peers
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int, term, lastLogTerm, lastLogIndex int) {
				args := RequestVoteArgs{
					Term:         term,
					LastLogTerm:  lastLogTerm,
					LastLogIndex: lastLogIndex,
					CandidateID:  rf.me,
				}
				var reply RequestVoteReply
				if !rf.sendRequestVote(server, &args, &reply) {
					return
				}
				// deal with response/reply
				if reply.VoteGranted {
					votesReceived.Add(1)
					if int(votesReceived.Load()) > len(rf.peers)/2 {
						rf.transitionToLeader()
						return
					}
				} else {
					if reply.Term > term {
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.mu.Unlock()
					}
				}
			}(i, term, lastLogTerm, lastLogIndex)
		}
	}
}

func (rf *Raft) transitionToLeader() {
	rf.mu.Lock()
	DPrintf("peer %d: becoming leader", rf.me)
	rf.state = Leader
	rf.mu.Unlock()
	go rf.sendHeartBeats()
}

func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		lastLogTerm := 0
		if len(rf.logs) > 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		args := AppendEntriesArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			PrevLogIndex:      rf.lastApplied,
			PrevLogTerm:       lastLogTerm,
			Entries:           nil,
			LeaderCommitIndex: rf.commitIndex,
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(server int) {
				var reply AppendEntriesReply
				if !rf.sendAppendEntries(server, &args, &reply) {
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = nil
				}
				rf.mu.Unlock()
			}(i)
		}

		time.Sleep(100 * time.Millisecond)
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.logs = make([]LogEntry, 0)
	rf.state = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]any, 0)
	rf.matchIndex = make([]any, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
