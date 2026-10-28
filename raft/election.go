package raft

import (
	"log"
	"math/rand/v2"
	"time"
)

// noVote marks votedFor as empty, since 0 could be a valid NodeID.
const noVote NodeID = -1

// Election timing (§5.2). Every reset of the election timer picks a new random
// timeout between min and max, so nodes rarely time out together and split the
// vote. Heartbeats must come well before the minimum timeout, so followers of
// a live leader never start an election.
const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

// randomElectionTimeout returns a duration in [electionTimeoutMin, electionTimeoutMax).
func randomElectionTimeout() time.Duration {
	return electionTimeoutMin + rand.N(electionTimeoutMax-electionTimeoutMin)
}

// ---- Message handlers ----

// requestVoteHandler answers a candidate asking for this node's vote
// (Figure 2, RequestVote RPC, receiver implementation). It rejects candidates
// from older terms, moves to newer terms, and grants at most one vote per term.
// Granting a vote resets the election timer, so this node doesn't start its own
// election while the candidate is collecting votes.
//
// The paper also checks that the candidate's log is up to date (§5.4.1); that
// comes with log replication.
func (n *Node) requestVoteHandler(from NodeID, args requestVoteArgs) {
	if args.term < n.term {
		n.send(from, requestVoteReply{term: n.term, voteGranted: false})
		return
	}

	// A newer term means a new election: step down and forget the vote from the old term.
	if args.term > n.term {
		n.stepDown(args.term)
	}

	// Only one vote per term.
	if n.votedFor != noVote && n.votedFor != args.candidateID {
		n.send(from, requestVoteReply{term: n.term, voteGranted: false})
		return
	}

	n.votedFor = args.candidateID
	n.electionTimer.Reset(randomElectionTimeout())
	n.send(from, requestVoteReply{term: n.term, voteGranted: true})
}

// requestVoteReplyHandler counts the answers to this node's vote requests
// (Figure 2, Rules for Servers, Candidates). Votes from a majority of the
// cluster, counting its own, make it leader. A reply with a newer term means
// this node is outdated, so it steps down.
func (n *Node) requestVoteReplyHandler(from NodeID, reply requestVoteReply) {
	if reply.term > n.term {
		n.stepDown(reply.term)
		return
	}

	// Ignore replies from an old election, or arriving after this one was decided.
	if n.role != candidate || reply.term != n.term || !reply.voteGranted {
		return
	}

	// A set, not a counter: a duplicated message must not count twice.
	n.votes[from] = true
	if len(n.votes) > (len(n.Peers)+1)/2 {
		n.becomeLeader()
	}
}

// appendEntriesHandler handles a heartbeat from a leader (Figure 2,
// AppendEntries RPC, receiver implementation, without log entries yet). A
// heartbeat from an older term is rejected, so an outdated leader learns about
// the newer term from the reply. Otherwise the sender is the legitimate leader:
// this node becomes or stays a follower and resets its election timer. That
// reset is what stops followers from starting elections while a leader lives.
func (n *Node) appendEntriesHandler(from NodeID, args appendEntriesArgs) {
	if args.term < n.term {
		n.send(from, appendEntriesReply{term: n.term})
		return
	}

	// A newer term means a new election: step down and forget the vote from the old term.
	if args.term > n.term {
		n.stepDown(args.term)
	}

	// The sender is the leader of this term: a candidate in the same term lost the election.
	n.role = follower
	n.electionTimer.Reset(randomElectionTimeout())
	n.send(from, appendEntriesReply{term: n.term})
}

// appendEntriesReplyHandler only checks the term for now: a reply with a newer
// term means this leader is outdated and steps down (Figure 2, Rules for
// Servers, All Servers). With log replication it will also track what each
// follower has stored.
func (n *Node) appendEntriesReplyHandler(reply appendEntriesReply) {
	if reply.term > n.term {
		n.stepDown(reply.term)
	}
}

// ---- Role transitions ----

// startElection runs when the election timer fires (§5.2; Figure 2, Rules for
// Servers, Candidates): become candidate, increment the term, vote for itself,
// reset the timer and ask every peer for its vote. If no one wins, the timer
// fires again and a new election starts in a higher term.
func (n *Node) startElection() {
	// A leader's timer is stopped, but a timeout may have fired just before winning.
	if n.role == leader {
		return
	}

	n.role = candidate
	n.term++
	n.votedFor = n.ID
	n.votes = map[NodeID]bool{n.ID: true}
	n.electionTimer.Reset(randomElectionTimeout())
	log.Printf("node %d: election timeout, candidate for term %d", n.ID, n.term)

	n.broadcast(requestVoteArgs{term: n.term, candidateID: n.ID})
}

// becomeLeader runs when a majority voted for this node. A leader doesn't run
// elections, so it stops its timer, and it sends heartbeats immediately so the
// other nodes learn about it (Figure 2, Rules for Servers, Leaders).
func (n *Node) becomeLeader() {
	n.role = leader
	n.electionTimer.Stop()
	log.Printf("node %d: leader for term %d", n.ID, n.term)

	// Announce the win right away instead of waiting for the next tick.
	n.sendHeartbeats()
}

// stepDown moves to a newer term as a follower, with no vote cast in it yet.
// It applies the rule for all servers in Figure 2: any message with a term
// newer than currentTerm makes the node update its term and become follower.
func (n *Node) stepDown(term int) {
	// A leader's timer is stopped, so it has to be restarted.
	if n.role == leader {
		n.electionTimer.Reset(randomElectionTimeout())
	}
	n.term = term
	n.role = follower
	n.votedFor = noVote
}

// ---- Heartbeats ----

// sendHeartbeats sends an empty AppendEntries to every peer. Run calls it on
// every heartbeat tick; it does nothing unless this node is leader (§5.2).
func (n *Node) sendHeartbeats() {
	if n.role != leader {
		return
	}
	n.broadcast(appendEntriesArgs{term: n.term})
}
