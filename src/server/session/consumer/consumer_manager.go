package consumer

import (
	"fmt"
	"sync"

	"tree_nity/src/ipc/id"
	"tree_nity/src/server/utils/hashmap"
)

// Manager safely stores metadata independently of connections; initialize with NewManager.
type Manager struct {
	mu            sync.RWMutex
	consumerTable *hashmap.HashMap[id.ID, *Info]
}

// NewManager returns an empty, initialized metadata manager.
func NewManager() *Manager {
	return &Manager{
		consumerTable: hashmap.NewHashMap[id.ID, *Info](),
	}
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

// GetConsumer returns a metadata copy with a new connection; the caller must Close it.
func (m *Manager) GetConsumer(id id.ID) (*Consumer, error) {
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

	connection, err := newConnection(consumerInfo.ipcPath)
	if err != nil {
		return nil, fmt.Errorf("connect consumer %q: %w", id, err)
	}

	return newConsumer(&consumerInfo, connection), nil
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

// Len returns the number of stored consumers, or zero for a nil or uninitialized manager.
func (m *Manager) Len() uint64 {
	if err := m.validate(); err != nil {
		return 0
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.consumerTable.Len()
}

func (m *Manager) validate() error {
	if m == nil || m.consumerTable == nil {
		return ErrManagerUninitialized
	}
	return nil
}
