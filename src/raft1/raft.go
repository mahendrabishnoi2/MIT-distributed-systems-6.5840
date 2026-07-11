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

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State
	currentTerm int
	votedFor    int // -1 sentinal value = not voted yet

	logs []LogEntry // stores logs - maybe later we make it pluggable

	commitIndex int // index of last log committed on the node (init = 0, monotonically increases)
	lastApplied int // index of last log applied to application/state machine (init = 0, monotonically increases)

	// leader volatile state (reinitilized on election)
	nextIndex  []int // for each peer/server, index of next log entry to send (init = last log index + 1)
	matchIndex []int // for each peer/server, highest log entry known to be replicated (init = 0, monotonically increases)

	// channel for raft event loop to consume from
	events  chan event
	applyCh chan raftapi.ApplyMsg

	// ticks related state - used for heartbeats and elections
	tickCh           chan struct{}
	tickDuration     time.Duration // duration for tick trigger
	heartbeatElapsed int           // number of ticks since last heartbeatTimeout (only leader)
	electionElapsed  int           // number of ticks since last electionTimeout

	heartbeatTimeout          int
	electionTimeout           int
	randomizedElectionTimeout int // timeout b/w [electionTimeout, 2*electionTimeout-1], updated on raft server state change

	// vote related state
	votes int
}

type GetStateReply struct {
	CurrentTerm int
	State       State
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	ch := make(chan any)
	rf.events <- event{
		kind:    evGetState,
		payload: nil,
		reply:   ch,
	}
	out := (<-ch).(*GetStateReply)
	return out.CurrentTerm, out.State == Leader
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
	CandidateID  int
	Term         int
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

type AppendEntriesRequest struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type appendEntriesReplyEvent struct {
	server       int
	requestTerm  int
	prevLogIndex int
	entriesCount int
	reply        AppendEntriesReply
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	ch := make(chan any, 1)
	rf.events <- event{
		kind:    evRequestVote,
		payload: args,
		reply:   ch,
	}
	*reply = (<-ch).(RequestVoteReply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	ch := make(chan any, 1)
	rf.events <- event{
		kind:    evAppendEntries,
		payload: args,
		reply:   ch,
	}
	*reply = (<-ch).(AppendEntriesReply)
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
	DPrintf("server %d: sending request vote to server %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type StartRequest struct {
	Command any
}

type StartReply struct {
	MaybeCommitIndex int
	Term             int
	IsLeader         bool
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
	// Your code here (3B).
	ch := make(chan any)
	rf.events <- event{
		kind:    evStart,
		payload: &StartRequest{Command: command},
		reply:   ch,
	}
	reply := (<-ch).(*StartReply)
	return reply.MaybeCommitIndex, reply.Term, reply.IsLeader
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.tickCh <- struct{}{}
		time.Sleep(rf.tickDuration)
	}
}

// run runs the event loop in a goroutine, processing events in a sequential order
func (rf *Raft) run() {
	for !rf.killed() {
		select {
		case ev := <-rf.events:
			rf.handleEvent(ev)
		case <-rf.tickCh:
			rf.handleTick()
		}
	}
}

func (rf *Raft) handleEvent(ev event) { // this is basically Step(), central entry point for raft state machine
	// DPrintf("server %d: received event kind: %v", rf.me, ev.kind)
	switch ev.kind {
	case evGetState:
		ev.reply <- &GetStateReply{
			CurrentTerm: rf.currentTerm,
			State:       rf.state,
		}
	case evRequestVote:
		args := ev.payload.(*RequestVoteArgs)
		reply := RequestVoteReply{}
		// if my term > requestor's term, no vote
		// if requestor's log is not as up to date as mine, no vote (5.4.1 Election Restriction)
		if args.Term < rf.currentTerm || !rf.isAtLeastAsUpToDate(args.LastLogTerm, args.LastLogIndex) {
			reply.Term = rf.currentTerm
			ev.reply <- reply
			return
		}

		if args.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
		}

		reply.Term = rf.currentTerm
		if rf.votedFor == args.CandidateID || rf.votedFor == -1 {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		}

		ev.reply <- reply
	case evRequestVoteReply:
		reply := ev.payload.(*RequestVoteReply)
		DPrintf("server %d: request vote reply: %+v", rf.me, reply)
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if rf.state != Candidate {
			return
		}
		if reply.VoteGranted && reply.Term == rf.currentTerm {
			rf.votes++
		}
		if rf.votes >= (len(rf.peers)/2)+1 {
			rf.becomeLeader()
		}
	case evAppendEntries:
		args := ev.payload.(*AppendEntriesRequest)
		reply := AppendEntriesReply{Term: rf.currentTerm}
		if args.Term < rf.currentTerm {
			ev.reply <- reply
			return
		}

		if args.Term > rf.currentTerm || rf.state != Follower {
			rf.becomeFollower(args.Term)
		}
		// A non-stale AppendEntries is evidence of a leader, even if the
		// log consistency check below fails.
		rf.electionElapsed = 0
		reply.Term = rf.currentTerm

		// The log must contain the entry immediately before the new entries,
		// with the same term.
		if args.PrevLogIndex < 0 || args.PrevLogIndex >= len(rf.logs) ||
			rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			ev.reply <- reply
			return
		}

		// Keep matching entries. At the first conflict, discard that entry
		// and everything after it, then append the leader's remaining entries.
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			if index < len(rf.logs) {
				if rf.logs[index].Term == entry.Term {
					continue
				}
				if index <= rf.commitIndex {
					// A committed entry must never be overwritten.
					ev.reply <- reply
					return
				}
				rf.logs = rf.logs[:index]
			}
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}

		if args.LeaderCommitIndex > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommitIndex, rf.lastLogIndex())
			rf.applyCommittedEntries()
		}

		reply.Success = true
		ev.reply <- reply
	case evAppendEntriesReply:
		response := ev.payload.(*appendEntriesReplyEvent)
		if response.reply.Term > rf.currentTerm {
			rf.becomeFollower(response.reply.Term)
			return
		}
		if rf.state != Leader || response.requestTerm != rf.currentTerm {
			return
		}

		server := response.server
		if response.reply.Success {
			matchedThrough := response.prevLogIndex + response.entriesCount
			if matchedThrough > rf.matchIndex[server] {
				rf.matchIndex[server] = matchedThrough
				rf.nextIndex[server] = matchedThrough + 1
			}
			rf.advanceCommitIndex()
		} else if rf.nextIndex[server] == response.prevLogIndex+1 && rf.nextIndex[server] > 1 {
			// Retry from an earlier point on the next replication attempt.
			rf.nextIndex[server]--
		}
	case evStart:
		args := ev.payload.(*StartRequest)
		if rf.state != Leader {
			ev.reply <- &StartReply{
				MaybeCommitIndex: -1,
				Term:             rf.currentTerm,
				IsLeader:         false,
			}
			return
		}

		logEntry := LogEntry{
			Term:    rf.currentTerm,
			Command: args.Command,
		}
		rf.logs = append(rf.logs, logEntry)
		index := rf.lastLogIndex()
		rf.matchIndex[rf.me] = index
		reply := &StartReply{
			MaybeCommitIndex: index,
			Term:             rf.currentTerm,
			IsLeader:         true,
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.sendAppendEntriesToPeer(i)
		}
		rf.advanceCommitIndex()
		ev.reply <- reply
	default:
		panic("unexpected event kind")
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	DPrintf("server %d: becoming follower in term %d", rf.me, term)
	rf.reset(term)
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	DPrintf("server %d: becoming leader in term %d", rf.me, rf.currentTerm)
	rf.reset(rf.currentTerm)
	lastIndex := rf.lastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastIndex
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	DPrintf("server %d: becoming candidate in term %d", rf.me, rf.currentTerm+1)
	rf.reset(rf.currentTerm + 1)
	rf.votedFor = rf.me
}

func (rf *Raft) isAtLeastAsUpToDate(lastLogTerm, lastLogIndex int) bool {
	if lastLogTerm > rf.lastLogTerm() {
		return true
	} else if lastLogTerm == rf.lastLogTerm() {
		return lastLogIndex >= rf.lastLogIndex()
	}
	return false
}

func (rf *Raft) buildHeartbeatArgs() AppendEntriesRequest {
	return AppendEntriesRequest{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		PrevLogIndex:      rf.lastLogIndex(),
		PrevLogTerm:       rf.lastLogTerm(),
		Entries:           []LogEntry{},
		LeaderCommitIndex: rf.commitIndex,
	}
}

func (rf *Raft) buildAppendEntriesReq(server int) AppendEntriesRequest {
	next := rf.nextIndex[server]
	if next < 1 {
		next = 1
	}
	prevIndex := next - 1
	entries := append([]LogEntry(nil), rf.logs[next:]...)
	return AppendEntriesRequest{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		PrevLogIndex:      prevIndex,
		PrevLogTerm:       rf.logs[prevIndex].Term,
		Entries:           entries,
		LeaderCommitIndex: rf.commitIndex,
	}
}

func (rf *Raft) handleTick() {
	if rf.state == Leader {
		rf.heartbeatElapsed++
		rf.electionElapsed++

		if rf.heartbeatElapsed >= rf.heartbeatTimeout {
			rf.heartbeatElapsed = 0

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				rf.sendAppendEntriesToPeer(i)
			}
		}
		return
	}

	rf.electionElapsed++
	if rf.electionElapsed < rf.randomizedElectionTimeout {
		return
	}
	rf.becomeCandidate()
	// rf.votes = 1 // for now, we are directly starting with vote = 1, maybe later send a vote on events channel

	for i := range rf.peers {
		if i == rf.me {
			rf.events <- event{
				kind: evRequestVoteReply,
				payload: &RequestVoteReply{
					Term:        rf.currentTerm,
					VoteGranted: true,
				},
			}
			continue
		}
		args := RequestVoteArgs{
			CandidateID:  rf.me,
			Term:         rf.currentTerm,
			LastLogTerm:  rf.logs[rf.lastLogIndex()].Term,
			LastLogIndex: rf.lastLogIndex(),
		}
		go func(server int, args RequestVoteArgs) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.events <- event{
					kind:    evRequestVoteReply,
					payload: &reply,
				}
			}
		}(i, args)
	}
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.logs[rf.lastLogIndex()].Term
}

func (rf *Raft) advanceCommitIndex() {
	for index := rf.lastLogIndex(); index > rf.commitIndex; index-- {
		if rf.logs[index].Term != rf.currentTerm {
			continue
		}
		replicated := 0
		for _, matchIndex := range rf.matchIndex {
			if matchIndex >= index {
				replicated++
			}
		}
		if replicated >= len(rf.peers)/2+1 {
			rf.commitIndex = index
			rf.applyCommittedEntries()
			return
		}
	}
}

func (rf *Raft) applyCommittedEntries() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		entry := rf.logs[rf.lastApplied]
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: rf.lastApplied,
		}
	}
}

func (rf *Raft) sendAppendEntriesToPeer(server int) {
	args := rf.buildAppendEntriesReq(server)
	response := &appendEntriesReplyEvent{
		server:       server,
		requestTerm:  args.Term,
		prevLogIndex: args.PrevLogIndex,
		entriesCount: len(args.Entries),
	}
	go func() {
		var reply AppendEntriesReply
		if rf.sendAppendEntries(server, &args, &reply) {
			response.reply = reply
			rf.events <- event{
				kind:    evAppendEntriesReply,
				payload: response,
			}
		}
	}()
}

func (rf *Raft) reset(term int) {
	if rf.currentTerm != term {
		rf.currentTerm = term
		rf.votedFor = -1
	}

	rf.heartbeatElapsed = 0
	rf.electionElapsed = 0

	rf.votes = 0
	rf.randomizeElectionTimeout()
	// todo: reset nextIndex and matchIndex as well
}

func (rf *Raft) randomizeElectionTimeout() {
	rf.randomizedElectionTimeout = rf.electionTimeout + rand.Intn(rf.electionTimeout)
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
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.logs = []LogEntry{{
		Term:    0,
		Command: nil,
	}}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	for i := range len(peers) {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.tickDuration = time.Millisecond * 50
	rf.heartbeatTimeout = 1
	rf.electionTimeout = 10

	rf.randomizedElectionTimeout = rf.electionTimeout + rand.Intn(rf.electionTimeout)

	rf.events = make(chan event, 64)
	rf.tickCh = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.run()

	return rf
}
