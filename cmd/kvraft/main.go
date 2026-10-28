package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/joaquin-salas/kv-raft/raft"
)

func main() {
	var clusterSize int = 3

	ids := make([]raft.NodeID, clusterSize)
	for i := range ids {
		ids[i] = raft.NodeID(i + 1)
	}

	transport := raft.NewChannelTransport(64)

	nodes := make([]*raft.Node, 0, clusterSize)
	for _, id := range ids {
		inbox := transport.Register(id)
		nodes = append(nodes, raft.NewNode(id, raft.PeersOf(id, ids), transport, inbox))
	}

	// Prevent main from exiting, only stop when an Interrupt signal arrives (Ctrl + C).
	// The same ctx stops the nodes.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Start only after every node is registered, so no message targets a missing node.
	for _, node := range nodes {
		go node.Run(ctx)
	}

	log.Printf("cluster of %d nodes running, press Ctrl+C to stop", clusterSize)
	<-ctx.Done()
}
