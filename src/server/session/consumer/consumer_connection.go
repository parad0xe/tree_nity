package consumer

import (
	"context"
	"fmt"
	"sync"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/protocol"
)

// Connection holds the transient resources for one consumer connection.
type Connection struct {
	// MessageChan carries messages to deliver.
	MessageChan chan protocol.Message
	// AcknowledgedChan carries acknowledged offsets.
	AcknowledgedChan chan uint32
	// HeartbeatChan carries received heartbeats.
	HeartbeatChan chan struct{}
	// Outbound sends protocol frames to the consumer.
	Outbound *ipc.Outbound

	done      <-chan struct{}
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// newConnection opens IPC with a cancellable context.
func newConnection(ctx context.Context, ipcPath string) (*Connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	outbound, err := ipc.NewOutbound(ipcPath, protocol.Encode)
	if err != nil {
		return nil, fmt.Errorf("open consumer outbound %q: %w", ipcPath, err)
	}

	connectionCtx, cancel := context.WithCancel(ctx)
	return &Connection{
		MessageChan:      make(chan protocol.Message),
		AcknowledgedChan: make(chan uint32),
		HeartbeatChan:    make(chan struct{}),
		Outbound:         outbound,
		done:             connectionCtx.Done(),
		cancel:           cancel,
	}, nil
}

// Done returns the cancellation signal, or nil when uninitialized.
func (c *Connection) Done() <-chan struct{} {
	if err := c.validate(); err != nil {
		return nil
	}
	return c.done
}

// Close cancels the context and closes the outbound once.
func (c *Connection) Close() error {
	if err := c.validate(); err != nil {
		return nil
	}
	if c.cancel != nil {
		c.cancel()
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
