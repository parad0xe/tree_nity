package consumer

import (
	"context"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/protocol"
)

// Connection holds the transient resources for one consumer connection.
// IPC initialization remains to be implemented.
type Connection struct {
	// Cancel requests termination of this connection's routines.
	// It is set when the connection is attached to a topic.
	Cancel           context.CancelFunc
	MessageChan      chan protocol.Message
	AcknowledgedChan chan uint32
	HeartbeatChan    chan struct{}
	Outbound         *ipc.Outbound
}

func newConnection(ipcPath string) *Connection {
	return &Connection{
		MessageChan:      make(chan protocol.Message),
		AcknowledgedChan: make(chan uint32),
		HeartbeatChan:    make(chan struct{}),
		Outbound:         ipc.NewOutbound(ipcPath, nil),
	}
}

func (c *Connection) Close() {
	if c == nil {
		return
	}
	if c.Cancel != nil {
		c.Cancel()
	}
}

func (c *Connection) validate() error {
	if c == nil {
		return ErrConnectionUninitialized
	}
	return nil
}
