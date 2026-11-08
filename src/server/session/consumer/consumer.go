package consumer

import (
	"sync"

	"tree_nity/src/ipc/id"
)

// Consumer holds metadata and an optional connection; it must not be copied.
type Consumer struct {
	// Info is a caller-owned metadata snapshot.
	Info Info

	mu         sync.Mutex
	connection *Connection
	manager    *Manager
	managedID  id.ID
}

func newConsumer(info *Info) *Consumer {
	return &Consumer{
		Info: *info,
	}
}

// Connection returns the current connection, or nil when disconnected.
// Its use must be coordinated with DisconnectConsumer.
func (c *Consumer) Connection() *Connection {
	if err := c.Validate(); err != nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connection
}

// Validate checks the consumer and its metadata.
func (c *Consumer) Validate() error {
	if c == nil {
		return ErrUninitialized
	}
	return c.Info.validate()
}
