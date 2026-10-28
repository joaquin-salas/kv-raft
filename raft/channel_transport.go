package raft

import "sync"

// ChannelTransport simulates a network inside one process: every node owns an
// inbox channel and Send drops messages into the target's inbox.
type ChannelTransport struct {
	mu           sync.RWMutex
	inboxes      map[NodeID]chan Message
	inboxSize    int
	disconnected map[NodeID]bool // nodes cut off from the network
}

// It's a compile time comprobation of is ChannelTransport a Transport?
var _ Transport = (*ChannelTransport)(nil)

// NewChannelTransport creates an empty network. Each inbox holds up to
// inboxSize messages; once full, new messages to that node are dropped.
func NewChannelTransport(inboxSize int) *ChannelTransport {
	return &ChannelTransport{
		inboxes:      make(map[NodeID]chan Message),
		inboxSize:    inboxSize,
		disconnected: make(map[NodeID]bool),
	}
}

// Register creates the inbox for id and returns it. The node only reads from it.
func (t *ChannelTransport) Register(id NodeID) <-chan Message {
	t.mu.Lock()
	defer t.mu.Unlock()

	inbox := make(chan Message, t.inboxSize)
	t.inboxes[id] = inbox
	return inbox
}

// Disconnect cuts id off from the network: nothing it sends or is sent to it
// gets through. The node keeps running and doesn't notice.
func (t *ChannelTransport) Disconnect(id NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.disconnected[id] = true
}

// Connect puts id back on the network.
func (t *ChannelTransport) Connect(id NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.disconnected, id)
}

// Send never blocks. Delivery happens in its own goroutine, so messages can
// arrive out of order. A message is dropped, like a lost packet, when either
// end is disconnected, the target is unknown, or its inbox is full.
func (t *ChannelTransport) Send(msg Message) {
	t.mu.RLock()
	inbox, ok := t.inboxes[msg.To]
	cutOff := t.disconnected[msg.From] || t.disconnected[msg.To]
	t.mu.RUnlock()
	if !ok || cutOff {
		return
	}

	go func() {
		select {
		case inbox <- msg:
		default:
		}
	}()
}
