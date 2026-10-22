package consumer

import (
	"context"
	"fmt"
	"sync"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/protocol"
)

// Connection holds the transient resources for one consumer connection.
// A protocol encoder is still required before connections can be opened.
type Connection struct {
	// Cancel requests termination of this connection's routines.
	// It is set when the connection is attached to a topic.
	Cancel           context.CancelFunc
	MessageChan      chan protocol.Message
	AcknowledgedChan chan uint32
	HeartbeatChan    chan struct{}
	Outbound         *ipc.Outbound
	closeOnce        sync.Once
}

func newConnection(ipcPath string) (*Connection, error) {
	// TODO: supply a protocol encoder once frame encoding is implemented.
	outbound, err := ipc.NewOutbound(ipcPath, nil)
	if err != nil {
		return nil, fmt.Errorf("open consumer outbound %q: %w", ipcPath, err)
	}

	return &Connection{
		MessageChan:      make(chan protocol.Message),
		AcknowledgedChan: make(chan uint32),
		HeartbeatChan:    make(chan struct{}),
		Outbound:         outbound,
	}, nil
}

// Close cancels the connection's workers and closes its outbound at most once.
// Only the call that closes the outbound can return a closure error.
func (c *Connection) Close() error {
	if err := c.validate(); err != nil {
		return nil
	}
	if c.Cancel != nil {
		c.Cancel()
	}
	var err error
	c.closeOnce.Do(func() {
		if c.Outbound != nil {
			if outboundErr := c.Outbound.Close(); outboundErr != nil {
				err = fmt.Errorf("close consumer outbound %q: %w", c.Outbound.IPCPath(), outboundErr)
			}
		}
	})
	return err
}

func (c *Connection) validate() error {
	if c == nil {
		return ErrConnectionUninitialized
	}
	return nil
}
