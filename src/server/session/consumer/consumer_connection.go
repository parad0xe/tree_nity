package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/protocol"
)

// Connection holds connection resources; it must not be copied.
type Connection struct {
	// MessageChan carries messages to deliver.
	MessageChan chan protocol.Message
	// AcknowledgedChan carries acknowledged offsets.
	AcknowledgedChan chan uint32
	// HeartbeatChan carries received heartbeats.
	HeartbeatChan chan struct{}

	mu        sync.RWMutex
	outbound  *ipc.Outbound
	ctx       context.Context
	done      <-chan struct{}
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
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
		outbound:         outbound,
		ctx:              connectionCtx,
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

// Send sends a frame, returning a context error when canceled or closed.
func (c *Connection) Send(ctx context.Context, data protocol.Data) error {
	return c.send(ctx, data, false)
}

// SendAhead sends ahead of queued frames, with the same cancellation as Send.
func (c *Connection) SendAhead(ctx context.Context, data protocol.Data) error {
	return c.send(ctx, data, true)
}

func (c *Connection) send(ctx context.Context, data protocol.Data, ahead bool) error {
	if err := c.validate(); err != nil {
		return err
	}
	if c.ctx == nil {
		return ErrConnectionUninitialized
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Keep the descriptor open for the entire send.
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.outbound == nil {
		return ErrConnectionUninitialized
	}
	if err := c.ctx.Err(); err != nil {
		return err
	}

	// Either context can interrupt a send waiting on a full queue.
	sendCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	var err error
	if ahead {
		err = c.outbound.SendAhead(sendCtx, data)
	} else {
		err = c.outbound.Send(sendCtx, data)
	}
	if errors.Is(err, context.Canceled) && c.ctx.Err() != nil {
		return c.ctx.Err()
	}
	return err
}

// Close cancels sends, waits for them, then closes IPC once.
func (c *Connection) Close() error {
	if err := c.validate(); err != nil {
		return nil
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.outbound != nil {
			if outboundErr := c.outbound.Close(); outboundErr != nil {
				c.closeErr = fmt.Errorf("close consumer outbound %q: %w", c.outbound.IPCPath(), outboundErr)
			}
		}
	})
	return c.closeErr
}

func (c *Connection) validate() error {
	if c == nil {
		return ErrConnectionUninitialized
	}
	return nil
}
