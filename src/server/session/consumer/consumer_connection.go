package consumer

import (
	"context"
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
	if c.done == nil {
		return ErrConnectionUninitialized
	}
	if c.closed() {
		return context.Canceled
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
	if c.closed() {
		return context.Canceled
	}

	sendCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		if ahead {
			errCh <- c.outbound.SendAhead(sendCtx, data)
		} else {
			errCh <- c.outbound.Send(sendCtx, data)
		}
	}()
	select {
	case err := <-errCh:
		return err
	case <-c.done:
	}
	cancel()
	return <-errCh
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

// closed reports whether shutdown was requested.
func (c *Connection) closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *Connection) validate() error {
	if c == nil {
		return ErrConnectionUninitialized
	}
	return nil
}
