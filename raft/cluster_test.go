package raft

import (
	"testing"
	"time"
)

const (
	// electionDeadline is how long a check waits for the cluster to settle.
	// Several election timeouts, so a few split votes don't fail the test.
	electionDeadline = 1 * time.Second
	pollInterval     = 20 * time.Millisecond
)

// cluster is a running set of nodes on their own ChannelTransport, plus what
// the test needs to drive and observe them.
type cluster struct {
	t         *testing.T
	transport *ChannelTransport
	nodes     map[NodeID]*Node
	connected map[NodeID]bool
}

// newCluster builds size nodes with ids 1..size, starts them, and stops them
// when the test ends.
func newCluster(t *testing.T, size int) *cluster {
	t.Helper()

	ids := make([]NodeID, size)
	for i := range ids {
		ids[i] = NodeID(i + 1)
	}

	c := &cluster{
		t:         t,
		transport: NewChannelTransport(64),
		nodes:     make(map[NodeID]*Node),
		connected: make(map[NodeID]bool),
	}
	for _, id := range ids {
		inbox := c.transport.Register(id)
		c.nodes[id] = NewNode(id, PeersOf(id, ids), c.transport, inbox)
		c.connected[id] = true
	}

	// t.Context() is cancelled when the test ends, right before cleanups run,
	// so the cleanup only has to wait for every node to finish.
	ctx := t.Context()
	for _, node := range c.nodes {
		go node.Run(ctx)
	}
	t.Cleanup(func() {
		for _, node := range c.nodes {
			<-node.stopped
		}
	})

	return c
}

// disconnect cuts id off the network. The checks ignore it until it's
// reconnected: what a cut-off node believes doesn't count.
func (c *cluster) disconnect(id NodeID) {
	c.transport.Disconnect(id)
	c.connected[id] = false
}

// connect puts id back on the network and back into the checks.
func (c *cluster) connect(id NodeID) {
	c.transport.Connect(id)
	c.connected[id] = true
}

// connectedStatuses returns the status of every connected, running node.
func (c *cluster) connectedStatuses() map[NodeID]status {
	statuses := make(map[NodeID]status)
	for id, node := range c.nodes {
		if !c.connected[id] {
			continue
		}
		if s, ok := node.Status(); ok {
			statuses[id] = s
		}
	}
	return statuses
}

// checkOneLeader waits until exactly one connected node is leader and returns
// it. Leaders in different terms can briefly coexist (an old leader that hasn't
// heard of the new term yet), so it keeps waiting until the old one steps down.
// Two leaders in the same term fail the test right away: that's the safety
// property Raft guarantees.
func (c *cluster) checkOneLeader() NodeID {
	c.t.Helper()

	deadline := time.Now().Add(electionDeadline)
	for time.Now().Before(deadline) {
		leaders := make(map[int]NodeID) // term -> leader
		for id, s := range c.connectedStatuses() {
			if s.role != leader {
				continue
			}
			if other, ok := leaders[s.term]; ok {
				c.t.Fatalf("term %d has two leaders: %d and %d", s.term, other, id)
			}
			leaders[s.term] = id
		}

		if len(leaders) == 1 {
			for _, id := range leaders {
				return id
			}
		}
		time.Sleep(pollInterval)
	}

	c.t.Fatalf("no single leader within %v: %v", electionDeadline, c.connectedStatuses())
	return 0
}

// checkNoLeader watches the connected nodes for electionDeadline and fails if
// any of them is leader at any point. Use it when no majority is connected.
func (c *cluster) checkNoLeader() {
	c.t.Helper()

	deadline := time.Now().Add(electionDeadline)
	for time.Now().Before(deadline) {
		for id, s := range c.connectedStatuses() {
			if s.role == leader {
				c.t.Fatalf("node %d is leader in term %d, but no majority is connected", id, s.term)
			}
		}
		time.Sleep(pollInterval)
	}
}

// checkTerms waits until every connected node is in the same term and returns it.
func (c *cluster) checkTerms() int {
	c.t.Helper()

	deadline := time.Now().Add(electionDeadline)
	for time.Now().Before(deadline) {
		term, agreed := -1, true
		for _, s := range c.connectedStatuses() {
			if term == -1 {
				term = s.term
			} else if s.term != term {
				agreed = false
				break
			}
		}
		if agreed {
			return term
		}
		time.Sleep(pollInterval)
	}

	c.t.Fatalf("connected nodes don't agree on a term within %v: %v", electionDeadline, c.connectedStatuses())
	return 0
}
