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

type progress struct {
	nextIndex  int // next log entry to send to the server
	matchIndex int // max known log entry replicated to server
}

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
	peerProgress map[int]progress

	lastHeartbeatAt time.Time

	applyCh chan raftapi.ApplyMsg
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
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("peer %d: (term: %d, lastLogTerm: %d, lastLogIndex: %d) received RequestVote RPC from peer %d, args: %+v", rf.me, rf.currentTerm, rf.logs[rf.lastApplied].Term, rf.lastApplied, args.CandidateID, *args)
	defer func() { DPrintf("peer %d: RequestVote RPC result to peer %d: %+v", rf.me, args.CandidateID, *reply) }()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.convertToFollower()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}
	reply.Term = rf.currentTerm
	if !rf.isCandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		DPrintf("peer %d log more up to date than candidate %d", rf.me, args.CandidateID)
		return
	}

	if rf.votedFor == nil || rf.votedFor == &args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateID
	}
}

// isCandidateLogUpToDate returns if candidate's last log term is higher than its own last log cLogTerm
// If both have same last log term, then it returns true if its last log index is not greater than candidate.
// This is useful in implementing Raft Election Restriction Property
//
// Note: This method should be called from within a lock (rf.mu)
func (rf *Raft) isCandidateLogUpToDate(cLogTerm, cLogIndex int) bool {
	if rf.logs[rf.lastApplied].Term > cLogTerm {
		return false
	}
	if rf.logs[rf.lastApplied].Term == cLogTerm {
		return rf.lastApplied <= cLogIndex
	}
	return true
}

// convertToFollower
//
// caller to make sure rf.mu is locked
func (rf *Raft) convertToFollower() {
	rf.state = Follower
	// if we implement some channel based ops like stop heartbeat sync etc, do those here
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

	// based on leader commit index, we commit our entries also

	// do we have the log just before 1st entry (PrevLogIndex)?
	if len(rf.logs) <= args.PrevLogIndex {
		return
	}
	// do the terms match for leader and follower (for the log just before 1st entry) - if yes, we can commit it
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm { // term didn't match for previous log
		return
	}

	for i := 1; i <= len(args.Entries); i++ {
		// does log exists and does term match
		if args.PrevLogIndex+i >= len(rf.logs) {
			break
		}
		if rf.logs[i+args.PrevLogIndex].Term == args.Entries[i-1].Term {
			continue
		}
		// if we are here, it means that at index args.PrevLogIndex + i, the term didn't match with leader so delete entries starting with it (or only keep till it)
		rf.logs = rf.logs[:i+args.PrevLogIndex] // only keep log till the terms are same with new entries
		break
	}

	// add new entries in the logs
	for j := (len(rf.logs) - 1) - args.PrevLogIndex; j < len(args.Entries); j++ {
		rf.logs = append(rf.logs, args.Entries[j])
	}
	rf.lastApplied = len(rf.logs) - 1
	reply.Success = true

	if len(args.Entries) > 0 {
		DPrintf("peer %d: applied log entries, logs: %d, applied: %d, %+v", rf.me, len(rf.logs), rf.lastApplied, reply)
	}

	// commit from rf.commitIndex till args.LeaderCommitIndex
	for i := rf.commitIndex + 1; i <= args.LeaderCommitIndex && i < len(rf.logs); i++ {
		applyChanMsg := raftapi.ApplyMsg{
			CommandValid:  true,
			Command:       rf.logs[i].Command.Data,
			CommandIndex:  i,
			SnapshotValid: false,
			Snapshot:      []byte{},
			SnapshotTerm:  0,
			SnapshotIndex: i,
		}
		rf.applyCh <- applyChanMsg
		DPrintf("peer %d: committed a message: %+v", rf.me, applyChanMsg)
	}
	rf.commitIndex = min(args.LeaderCommitIndex, len(rf.logs)-1)
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
	// DPrintf("peer %d: sending appendentries rpc", rf.me)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return len(rf.logs), rf.currentTerm, false
	}
	index := len(rf.logs)
	term := rf.currentTerm

	DPrintf("leader %d: received a commnd: %v", rf.me, command)
	// Your code here (3B).
	//
	// in background send AppendRPC to all peers
	// manage some state related to indexes
	// return wihtout waiting for appendrpcs to be done
	// todo: figure out the index, prev log index, start = 0 or 1 etc, index out of bounds cases and so on

	logEntry := LogEntry{
		Term:    term,
		Command: RaftCommand{command},
	}
	rf.logs = append(rf.logs, logEntry)
	rf.lastApplied++

	return index, term, true
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
		lastLogTerm := rf.logs[rf.lastApplied].Term
		lastLogIndex := rf.lastApplied
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
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	DPrintf("peer %d: becoming leader", rf.me)
	rf.state = Leader

	rf.peerProgress = make(map[int]progress, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.peerProgress[i] = progress{
			nextIndex:  rf.lastApplied + 1,
			matchIndex: 0,
		}
	}
	DPrintf("leader %d: progress: %+v", rf.me, rf.peerProgress)

	rf.mu.Unlock()
	go rf.sendHeartBeats()
	go rf.syncFollowers()
}

func wgDoneChan(wg *sync.WaitGroup, x chan struct{}) {
	wg.Wait()
	x <- struct{}{}
}

func (rf *Raft) syncFollowers() {
	DPrintf("leader %d: syncing followers", rf.me)
	defer func() {
		DPrintf("leader %d: stop syncing followers", rf.me)
	}()
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			DPrintf("peer %d: not a leader", rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		var wg sync.WaitGroup
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			rf.mu.Lock()
			// determine what to send to a peer
			peerProgress := rf.peerProgress[i]
			logsToSend := []LogEntry{}

			if len(rf.logs) <= peerProgress.nextIndex { // do we have anything to send or not
				rf.mu.Unlock()
				continue
			}
			// we have something to send to this peer
			n := min(10, len(rf.logs)-peerProgress.nextIndex)
			if n == 0 && peerProgress.matchIndex != rf.commitIndex {
				peerProgress.nextIndex--
				n = 1
			}
			for x := range n {
				logsToSend = append(logsToSend, rf.logs[peerProgress.nextIndex+x])
			}
			DPrintf("leader %d: sending logs to follower %d: progress: %+v, num_logs:%d", rf.me, i, peerProgress, len(logsToSend))
			args := AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      peerProgress.nextIndex - 1,
				PrevLogTerm:       rf.logs[peerProgress.nextIndex-1].Term,
				Entries:           logsToSend,
				LeaderCommitIndex: rf.commitIndex,
			}
			var reply AppendEntriesReply
			rf.mu.Unlock()

			// run these rpcs in parallel and in goroutines so that in case of partition these are non blocking
			wg.Add(1)
			go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				defer wg.Done()

				if !rf.sendAppendEntries(server, args, reply) {
					return
				}

				DPrintf("leader %d: response from peer: %d: %+v", rf.me, server, reply)
				rf.mu.Lock()
				// is it possible that the node is not even leader anymore?
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = nil
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					rf.peerProgress[server] = progress{
						nextIndex:  peerProgress.nextIndex + len(logsToSend),
						matchIndex: peerProgress.nextIndex + len(logsToSend) - 1,
					}
					rf.maybeAdvanceCommitIndex()
				} else {
					rf.peerProgress[server] = progress{
						nextIndex: max(1, peerProgress.nextIndex-len(logsToSend)),
					}
					DPrintf("leader %d: updated progress for peer %d, %+v", rf.me, server, rf.peerProgress[server])
				}
				rf.mu.Unlock()
			}(i, &args, &reply)
		}
		wgDCh := make(chan struct{})
		go wgDoneChan(&wg, wgDCh)
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-wgDCh:
		case <-timer.C:
		}
	}
}

// maybeAdvanceCommitIndex
// make sure rf.mu is held when this method is called
func (rf *Raft) maybeAdvanceCommitIndex() {
	// scan from most recent log till the first non committed log entry
	for n := len(rf.logs) - 1; n > rf.commitIndex; n-- {
		// with counting approach, leader can only decide for rf.currentTerm
		if rf.logs[n].Term != rf.currentTerm {
			continue // should we return or break here instead?
		}

		// count match index for peers
		replicas := 1 // leader has this entry
		for _, peerProgress := range rf.peerProgress {
			if peerProgress.matchIndex >= n {
				replicas++
			}
		}

		if replicas >= (len(rf.peers)/2)+1 { // quorum
			// from rf.commitIndex till n, we can commit
			for k := rf.commitIndex + 1; k <= n; k++ {
				applyMsg := raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[k].Command.Data,
					CommandIndex: k,
				}
				rf.applyCh <- applyMsg
			}
			rf.commitIndex = n
			break
		}
	}
}

func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		lastLogTerm := rf.logs[len(rf.logs)-1].Term
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
				if reply.Success != true {
					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.currentTerm = reply.Term
						rf.votedFor = nil
					}
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
	rf.logs = []LogEntry{
		{
			Term:    0,
			Command: RaftCommand{nil},
		},
	}
	rf.state = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
