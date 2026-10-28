// Package raft implements leader election from the Raft consensus algorithm,
// as described in "In Search of an Understandable Consensus Algorithm"
// (Ongaro and Ousterhout). Section numbers like §5.2 and references to
// Figure 2 in the comments point to that paper.
//
// # Pieces
//
//   - [Node] is one member of the cluster. In every term it is a follower,
//     a candidate or the leader, and it decides what to do on its own.
//   - [Message] carries one of four payloads between nodes: a vote request,
//     a vote reply, a heartbeat (an AppendEntries with no entries) and a
//     heartbeat reply.
//   - [Transport] is how a node sends messages. Like a real network it only
//     promises to try: messages can arrive late, out of order, or never.
//   - [ChannelTransport] is an in-memory Transport that runs a whole cluster
//     in one process, and can cut nodes off to simulate failures.
//
// # Concurrency
//
// Each node runs in its own goroutine, [Node.Run], an event loop that handles
// one thing at a time: a message from its inbox, its election timer, or its
// heartbeat ticker. Only that goroutine touches the node's state, so there are
// no locks. Other goroutines read the state with [Node.Status], which asks the
// loop for a copy.
//
// Communication is one-way: a request and its reply are two separate messages,
// and no node ever waits for an answer. Lost messages need no retries: if an
// election gets no majority, the election timer simply starts another one.
//
// # Wiring a cluster
//
// Register every node with the transport before starting any of them, so no
// message is sent to a node the transport doesn't know yet:
//
//	transport := raft.NewChannelTransport(64)
//	nodes := make([]*raft.Node, 0, len(ids))
//	for _, id := range ids {
//		inbox := transport.Register(id)
//		nodes = append(nodes, raft.NewNode(id, raft.PeersOf(id, ids), transport, inbox))
//	}
//	for _, node := range nodes {
//		go node.Run(ctx)
//	}
//
// # Files
//
//	node.go               Node, its event loop, Status, sending
//	election.go           the election rules: handlers and role changes
//	message.go            Message and its payload types
//	transport.go          the Transport interface
//	channel_transport.go  the in-memory network for the demo and the tests
//	cluster_test.go       test helpers: build a cluster, cut nodes off, check leaders
//	election_test.go      election scenarios
package raft
