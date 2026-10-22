package consumer

import (
	"sync"

	"tree_nity/src/server/utils"
	"tree_nity/src/server/utils/hashmap"
)

// Manager stores consumer metadata independently of active connections.
type Manager struct {
	mu            sync.RWMutex
	consumerTable *hashmap.HashMap[id.ID, *Info]
}

func NewManager() *Manager {
	return &Manager{
		consumerTable: hashmap.NewHashMap[id.ID, *Info](),
	}
}

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
// It returns ErrNotFound when the consumer ID is absent from the manager.
func (m *Manager) GetConsumer(id id.ID) (*Consumer, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	info, exists := m.consumerTable.Get(id)
	if !exists {
		return nil, ErrNotFound
	}

	return newConsumer(info, newConnection(info.ipcPath)), nil
}

// RemoveConsumer deletes stored metadata; active connections belong to their topics.
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
