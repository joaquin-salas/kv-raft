package raft

import (
	"math/rand/v2"
	"testing"
	"time"
)

// TestInitialElection checks that a healthy cluster elects a leader and then
// stays quiet: with no failures, nobody should start another election.
func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)

	c.checkOneLeader()
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d after an election, want at least 1", term1)
	}

	time.Sleep(2 * electionDeadline)

	term2 := c.checkTerms()
	if term2 != term1 {
		t.Errorf("term changed from %d to %d with no failures", term1, term2)
	}
	c.checkOneLeader()
}

// TestReElection cuts off leaders and majorities and checks that the cluster
// recovers when it can, and elects no one when it can't.
func TestReElection(t *testing.T) {
	const size = 3
	c := newCluster(t, size)

	// The leader disappears: the other two elect a new one.
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.checkOneLeader()

	// The old leader comes back: it must step down, still one leader.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// Cut off the leader and one follower: the last node has no majority.
	// (Cutting off only followers wouldn't work: a lone leader never steps down.)
	follower := leader2%size + 1
	c.disconnect(leader2)
	c.disconnect(follower)
	c.checkNoLeader()

	// The follower comes back: two of three is a majority again.
	c.connect(follower)
	c.checkOneLeader()

	// Everyone is back: still one leader.
	c.connect(leader2)
	c.checkOneLeader()
}

// TestManyElections repeatedly cuts off 3 random nodes of 7. The remaining 4
// are a majority, so they must always end up with a leader.
func TestManyElections(t *testing.T) {
	const size = 7
	c := newCluster(t, size)
	c.checkOneLeader()

	for range 10 {
		cut := rand.Perm(size)[:3]
		for _, i := range cut {
			c.disconnect(NodeID(i + 1))
		}

		c.checkOneLeader()

		for _, i := range cut {
			c.connect(NodeID(i + 1))
		}
	}

	c.checkOneLeader()
}
