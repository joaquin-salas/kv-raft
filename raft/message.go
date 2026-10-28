package raft

// Message is the envelope that travels between nodes, like a network packet.
// Payload is one of the four message types below.
type Message struct {
	From    NodeID
	To      NodeID
	Payload any
}

// requestVoteArgs is sent by a candidate to ask for a vote (Figure 2,
// RequestVote RPC, arguments). lastLogIndex and lastLogTerm come with the log.
type requestVoteArgs struct {
	term        int
	candidateID NodeID
}

// requestVoteReply answers a requestVoteArgs. term lets an outdated candidate
// find out and step down.
type requestVoteReply struct {
	term        int
	voteGranted bool
}

// appendEntriesArgs is sent by the leader (Figure 2, AppendEntries RPC,
// arguments). With no log entries it is a heartbeat. leaderID and the log
// fields come with log replication.
type appendEntriesArgs struct {
	term int
}

// appendEntriesReply answers an appendEntriesArgs. term lets an outdated
// leader find out and step down; success comes with log replication.
type appendEntriesReply struct {
	term int
}
