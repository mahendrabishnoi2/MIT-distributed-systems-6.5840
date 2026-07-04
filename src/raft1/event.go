package raft

type event struct {
	kind    eventKind
	payload any
	reply   chan any
}

type eventKind int

const (
	evRequestVote eventKind = iota
	evRequestVoteReply
	evAppendEntries
	evAppendEntriesReply
	evStart
	evTick
)
