package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"tree_nity/src/ipc/id"
	"tree_nity/src/server/utils/hashmap"
)

// Manager synchronizes metadata and connection reservations.
type Manager struct {
	mu            sync.RWMutex
	consumerTable *hashmap.HashMap[id.ID, *Info]
	// activeConsumers reserves IDs until their connections are closed.
	activeConsumers *hashmap.HashMap[id.ID, *Consumer]
}

// NewManager returns an empty, initialized metadata manager.
func NewManager() *Manager {
	return &Manager{
		consumerTable:   hashmap.NewHashMap[id.ID, *Info](),
		activeConsumers: hashmap.NewHashMap[id.ID, *Consumer](),
	}
}

// Subscribe connects a consumer; nil resumes only within the same topic.
// previous is nil for a new consumer.
func (m *Manager) Subscribe(ctx context.Context, id id.ID, topic string, offset *uint32, prefix, ipcPath string) (*Consumer, *Info, error) {
	if err := m.validate(); err != nil {
		return nil, nil, err
	}
	if err := id.Validate(); err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	// Keep metadata changes and the connection reservation atomic.
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, active := m.activeConsumers.Get(id); active {
		return nil, nil, ErrAlreadyConnected
	}

	var previous *Info
	stored, exists := m.consumerTable.Get(id)
	if exists {
		snapshot := *stored
		previous = &snapshot
	} else {
		if err := m.addConsumer(id, topic, 0, prefix, ipcPath); err != nil {
			return nil, nil, err
		}
		stored, _ = m.consumerTable.Get(id)
	}
	if offset == nil && stored.Topic() != topic {
		var startOffset uint32
		offset = &startOffset
	}
	stored.updateInfo(&topic, offset, &prefix, &ipcPath)

	c, err := m.getConsumer(id)
	if err == nil {
		err = m.connectConsumer(ctx, c)
	}
	if err != nil {
		if previous == nil {
			err = errors.Join(err, m.consumerTable.Remove(id))
		} else {
			*stored = *previous
		}
		return nil, nil, err
	}
	return c, previous, nil
}

// Len returns the number of stored consumers, or zero for a nil or uninitialized manager.
func (m *Manager) Len() uint64 {
	if err := m.validate(); err != nil {
		return 0
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.consumerTable.Len()
}

// GetConsumer returns a disconnected snapshot of stored metadata.
func (m *Manager) GetConsumer(id id.ID) (*Consumer, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getConsumer(id)
}

// AddConsumer validates and stores metadata without opening a connection.
func (m *Manager) AddConsumer(id id.ID, topic string, offset uint32, prefix string, ipcPath string) error {
	if err := m.validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.addConsumer(id, topic, offset, prefix, ipcPath)
}

// ConnectConsumer reserves the ID and opens its current queue, tied to ctx.
func (m *Manager) ConnectConsumer(ctx context.Context, c *Consumer) error {
	if err := m.validate(); err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return err
	}

	// Serialize opening with disconnection for this consumer.
	c.mu.Lock()
	defer c.mu.Unlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connectConsumer(ctx, c)
}

// DisconnectConsumer closes a consumer connection and releases its ID.
// In-flight sends finish before the ID is released.
func (m *Manager) DisconnectConsumer(c *Consumer) error {
	if err := m.validate(); err != nil {
		return err
	}
	// Invalid metadata must not prevent cleanup.
	if err := c.Validate(); errors.Is(err, ErrUninitialized) {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connection == nil {
		return nil
	}
	if c.manager != m {
		return ErrNotFound
	}

	// Keep the ID reserved until the outbound has been closed.
	err := c.connection.Close()
	c.connection = nil
	m.mu.Lock()
	defer m.mu.Unlock()
	if owner, exists := m.activeConsumers.Get(c.managedID); exists && owner == c {
		err = errors.Join(err, m.activeConsumers.Remove(c.managedID))
	}
	return err
}

// UpdateInfo replaces supplied metadata fields together; nil preserves a field.
func (m *Manager) UpdateInfo(id id.ID, topic *string, offset *uint32, prefix, ipcPath *string) error {
	if err := m.validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	info, exists := m.consumerTable.Get(id)
	if !exists {
		return ErrNotFound
	}

	info.updateInfo(topic, offset, prefix, ipcPath)
	return nil
}

// RemoveConsumer deletes metadata without closing active connections.
func (m *Manager) RemoveConsumer(id id.ID) error {
	if err := m.validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.consumerTable.Get(id)
	if !exists {
		return ErrNotFound
	}

	return m.consumerTable.Remove(id)
}

// getConsumer requires m.mu to be held for reading.
func (m *Manager) getConsumer(id id.ID) (*Consumer, error) {
	info, exists := m.consumerTable.Get(id)
	if !exists {
		return nil, ErrNotFound
	}
	consumerInfo := *info
	c := newConsumer(&consumerInfo)
	c.manager, c.managedID = m, id
	return c, nil
}

// addConsumer requires m.mu to be held for writing.
func (m *Manager) addConsumer(id id.ID, topic string, offset uint32, prefix, ipcPath string) error {
	info := newInfo(id, topic, offset, prefix, ipcPath)
	if err := info.validate(); err != nil {
		return err
	}
	return m.consumerTable.Insert(info.id, info)
}

// connectConsumer requires m.mu and exclusive access to c.
func (m *Manager) connectConsumer(ctx context.Context, c *Consumer) error {
	if c.manager != m {
		return ErrNotFound
	}
	if c.connection != nil {
		return ErrAlreadyConnected
	}
	info, exists := m.consumerTable.Get(c.managedID)
	if !exists {
		return ErrNotFound
	}
	if _, active := m.activeConsumers.Get(c.managedID); active {
		return ErrAlreadyConnected
	}
	if err := m.activeConsumers.Insert(c.managedID, c); err != nil {
		return err
	}

	// Opening an existing queue does not wait for queue capacity.
	connection, err := newConnection(ctx, info.ipcPath)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		if connection != nil {
			err = errors.Join(err, connection.Close())
		}
		err = errors.Join(err, m.activeConsumers.Remove(c.managedID))
		return fmt.Errorf("connect consumer %q: %w", c.managedID, err)
	}
	c.connection = connection
	return nil
}

func (m *Manager) validate() error {
	if m == nil || m.consumerTable == nil || m.activeConsumers == nil {
		return ErrManagerUninitialized
	}
	return nil
}
