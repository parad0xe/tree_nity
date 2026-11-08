package consumer

import (
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

// Len returns the number of stored consumers, or zero for a nil or uninitialized manager.
func (m *Manager) Len() uint64 {
	if err := m.validate(); err != nil {
		return 0
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.consumerTable.Len()
}

// GetOrCreateConsumer returns a disconnected consumer from stored metadata.
func (m *Manager) GetOrCreateConsumer(id id.ID) (*Consumer, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	info, exists := m.consumerTable.Get(id)
	if !exists {
		m.mu.RUnlock()
		return nil, ErrNotFound
	}
	consumerInfo := *info
	m.mu.RUnlock()

	c := newConsumer(&consumerInfo)
	c.manager = m
	c.managedID = id
	return c, nil
}

// AddConsumer validates and stores metadata without opening a connection.
func (m *Manager) AddConsumer(id id.ID, topic string, offset uint32, prefix string, ipcPath string) error {
	if err := m.validate(); err != nil {
		return err
	}

	info := newInfo(id, topic, offset, prefix, ipcPath)
	if err := info.validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consumerTable.Insert(info.id, info)
}

// ConnectConsumer opens a consumer connection and reserves its ID.
func (m *Manager) ConnectConsumer(c *Consumer) error {
	if err := m.validate(); err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return err
	}

	// Serialize opening with disconnection for this consumer.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.manager != m {
		return ErrNotFound
	}
	if c.connection != nil {
		return ErrAlreadyConnected
	}

	m.mu.Lock()
	if _, exists := m.consumerTable.Get(c.managedID); !exists {
		m.mu.Unlock()
		return ErrNotFound
	}
	if _, exists := m.activeConsumers.Get(c.managedID); exists {
		m.mu.Unlock()
		return ErrAlreadyConnected
	}
	err := m.activeConsumers.Insert(c.managedID, c)
	m.mu.Unlock()
	if err != nil {
		return err
	}

	connection, err := newConnection(c.Info.ipcPath)
	if err != nil {
		m.mu.Lock()
		err = errors.Join(err, m.activeConsumers.Remove(c.managedID))
		m.mu.Unlock()
		return fmt.Errorf("connect consumer %q: %w", c.managedID, err)
	}
	c.connection = connection
	return nil
}

// DisconnectConsumer closes a consumer connection and releases its ID.
// The caller must first stop and wait for its workers.
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

// UpdateOffset stores the next topic offset without incrementing it.
func (m *Manager) UpdateOffset(id id.ID, offset uint32) error {
	if err := m.validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	info, exists := m.consumerTable.Get(id)
	if !exists {
		return ErrNotFound
	}

	info.offset = offset
	return nil
}

// UpdateTopic changes the stored subscription topic, preserving other metadata.
func (m *Manager) UpdateTopic(id id.ID, topic string) error {
	if err := m.validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	info, exists := m.consumerTable.Get(id)
	if !exists {
		return ErrNotFound
	}

	info.topic = topic
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

func (m *Manager) validate() error {
	if m == nil || m.consumerTable == nil || m.activeConsumers == nil {
		return ErrManagerUninitialized
	}
	return nil
}
