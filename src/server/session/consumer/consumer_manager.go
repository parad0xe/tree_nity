package consumer

import (
	"fmt"
	"sync"

	"tree_nity/src/server/utils"
	"tree_nity/src/server/utils/hashmap"
)

// Manager stores consumer metadata independently of active connections.
// Its methods are safe for concurrent use. Initialize it with NewManager.
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

// AddConsumer stores metadata after validating the consumer ID, without opening a connection.
// It returns hashmap.ErrKeyExists when the ID is already registered, or
// ErrManagerUninitialized when the manager is nil or uninitialized.
func (m *Manager) AddConsumer(id id.ID, offset uint32, prefix string, ipcPath string) error {
	if err := m.validate(); err != nil {
		return err
	}

	info := newInfo(id, offset, prefix, ipcPath)
	if err := info.validate(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consumerTable.Insert(info.id, info)
}

// GetConsumer returns a copy of the stored metadata with a new connection.
// The caller owns the connection and must close it when it is no longer needed.
// It returns ErrNotFound when the consumer ID is absent, or ErrManagerUninitialized
// when the manager is nil or uninitialized. The consumer's queue must already exist.
// Connection initialization errors leave the stored metadata unchanged.
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

// RemoveConsumer deletes stored metadata; active connections belong to their topics.
// It returns ErrNotFound when the consumer ID is absent, or
// ErrManagerUninitialized when the manager is nil or uninitialized.
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
