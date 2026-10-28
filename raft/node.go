package raft

import (
	"context"
	"log"
	"time"
)

// NodeID identifies a node in the cluster.
type NodeID int

// role is what a node does in its current term (§5.1). Every node starts as a
// follower; only a candidate can become leader.
type role string

const (
	follower  role = "follower"  // follows a leader and votes in elections
	candidate role = "candidate" // is running an election to become leader
	leader    role = "leader"    // won an election, sends heartbeats to keep its place
)

// Node is one member of a Raft cluster. It decides everything by itself from
// the messages it receives and its own timers.
//
// Node state is only touched by the Run goroutine, so it needs no lock. Other
// goroutines talk to a node through messages in its inbox, or read it with Status.
type Node struct {
	ID            NodeID
	Peers         []NodeID // every other node in the cluster
	role          role
	term          int             // currentTerm in Figure 2: the latest term this node has seen
	electionTimer *time.Timer     // fires when a follower or candidate hears nothing for too long (§5.2)
	votedFor      NodeID          // who this node voted for in term, or noVote (Figure 2)
	votes         map[NodeID]bool // voters in the current election, only used as candidate
	transport     Transport       // how this node sends messages
	inbox         <-chan Message  // where messages for this node arrive

	statusRequests chan chan status // Status() asks Run for a snapshot through here
	stopped        chan struct{}    // closed when Run returns
}

// status is a snapshot of a node's state, safe to use outside Run.
type status struct {
	role role
	term int
}

// NewNode creates a follower in term 0 that hasn't voted. It sends through
// transport and receives from inbox, which comes from the same transport (see
// ChannelTransport.Register). Its election timer starts counting right away,
// but nothing happens until Run is called.
func NewNode(id NodeID, peers []NodeID, transport Transport, inbox <-chan Message) *Node {
	return &Node{
		ID:            id,
		Peers:         peers,
		role:          follower,
		term:          0,
		electionTimer: time.NewTimer(randomElectionTimeout()),
		votedFor:      noVote,
		transport:     transport,
		inbox:         inbox,

		statusRequests: make(chan chan status),
		stopped:        make(chan struct{}),
	}
}

// ---- Event loop ----

// Run is the node's event loop: every message and timeout is handled here, one at a time.
// It runs until ctx is cancelled.
func (n *Node) Run(ctx context.Context) {
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	defer n.electionTimer.Stop()
	defer close(n.stopped)

	for {
		select {
		case <-ctx.Done():
			return
		case reply := <-n.statusRequests:
			reply <- status{role: n.role, term: n.term}
		case msg := <-n.inbox:
			n.handleMessage(msg)
		case <-n.electionTimer.C:
			n.startElection()
		case <-heartbeat.C:
			n.sendHeartbeats()
		}
	}
}

// handleMessage calls the handler for the message's payload type.
func (n *Node) handleMessage(msg Message) {
	switch p := msg.Payload.(type) {
	case requestVoteArgs:
		n.requestVoteHandler(msg.From, p)
	case requestVoteReply:
		n.requestVoteReplyHandler(msg.From, p)
	case appendEntriesArgs:
		n.appendEntriesHandler(msg.From, p)
	case appendEntriesReply:
		n.appendEntriesReplyHandler(p)
	default:
		log.Printf("node %d: unknown payload %T from node %d", n.ID, msg.Payload, msg.From)
	}
}

// ---- Status ----

// Status returns a snapshot of the node's role and term. It can be called from
// any goroutine because Run is the one reading the state. ok is false if the
// node has stopped.
func (n *Node) Status() (s status, ok bool) {
	reply := make(chan status, 1)
	select {
	case n.statusRequests <- reply:
		return <-reply, true
	case <-n.stopped:
		return status{}, false
	}
}

// ---- Sending ----

// send wraps payload in a Message from this node and hands it to the transport.
// It doesn't wait: any reply arrives later as a separate message.
func (n *Node) send(to NodeID, payload any) {
	n.transport.Send(Message{From: n.ID, To: to, Payload: payload})
}

// broadcast sends payload to every peer, one message each.
func (n *Node) broadcast(payload any) {
	for _, peer := range n.Peers {
		n.send(peer, payload)
	}
}

// ---- Utilities ----

// PeersOf returns every id in ids except id itself.
func PeersOf(id NodeID, ids []NodeID) []NodeID {
	peers := make([]NodeID, 0, len(ids)-1)
	for _, other := range ids {
		if other != id {
			peers = append(peers, other)
		}
	}
	return peers
}
