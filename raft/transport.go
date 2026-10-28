package raft

// Transport is how a node sends messages to other nodes. Receiving isn't part
// of it: each implementation hands a node its inbox when the node joins (see
// ChannelTransport.Register), the way a real server opens a socket at startup.
type Transport interface {
	// Send hands msg to the network and returns immediately. Like a real
	// network it promises nothing: the message may arrive late, out of order,
	// or never, and a reply is just another Message sent back by the receiver.
	Send(msg Message)
}
