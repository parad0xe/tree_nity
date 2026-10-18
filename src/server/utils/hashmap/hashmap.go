package hashmap

import (
	"errors"
)

// Hashable constrains keys to comparable values that provide a hash.
// A key's hash and equality behavior must remain stable while it is stored.
type Hashable interface {
	comparable
	// Hash returns a deterministic hash; equal keys must have equal hashes.
	Hash() uint64
}

const initialCapacity = 8

// HashMap stores key-value pairs using open addressing to resolve collisions.
// Initialize a HashMap with NewHashMap before inserting values.
// A HashMap must not be copied after initialization. Concurrent access involving
// writes requires external synchronization.
type HashMap[K Hashable, V any] struct {
	entries       []entry[K, V]
	occupiedSlots uint64
	nonEmptySlots uint64
}

// NewHashMap returns an empty HashMap ready for use.
func NewHashMap[K Hashable, V any]() *HashMap[K, V] {
	return &HashMap[K, V]{
		entries: make([]entry[K, V], initialCapacity),
	}
}

// Insert associates value with key, growing the table when needed.
// It returns an error if key already exists or m is nil or uninitialized.
func (m *HashMap[K, V]) Insert(key K, value V) error {
	if err := m.validate(); err != nil {
		return err
	}

	if !m.hasSpace() {
		if err := m.resize(); err != nil {
			return err
		}
	}

	index, exists := m.findSlot(key)
	if exists {
		return errors.New("key already exists in hashmap")
	}

	if m.entries[index].state == slotEmpty {
		m.nonEmptySlots++
	}

	m.entries[index] = entry[K, V]{
		key:   key,
		value: value,
		state: slotOccupied,
	}
	m.occupiedSlots++

	return nil
}

// Get returns the value associated with key and whether key is present.
// If key is absent or m is nil or uninitialized, it returns the zero value
// of V and false.
func (m *HashMap[K, V]) Get(key K) (V, bool) {
	var zero V

	if err := m.validate(); err != nil {
		return zero, false
	}

	index, exists := m.findSlot(key)
	if !exists {
		return zero, false
	}

	return m.entries[index].value, true
}

// Remove deletes key and its associated value.
// It returns an error if key is absent or m is nil or uninitialized.
func (m *HashMap[K, V]) Remove(key K) error {
	if err := m.validate(); err != nil {
		return err
	}

	index, exists := m.findSlot(key)
	if !exists {
		return errors.New("key not found in hashmap")
	}

	m.entries[index] = entry[K, V]{state: slotDeleted}
	m.occupiedSlots--
	return nil
}

// Len returns the number of stored key-value pairs.
// It returns zero if m is nil or uninitialized.
func (m *HashMap[K, V]) Len() uint64 {
	if err := m.validate(); err != nil {
		return 0
	}
	return m.occupiedSlots
}

func (m *HashMap[K, V]) resize() error {
	if err := m.validate(); err != nil {
		return err
	}

	oldEntries := m.entries
	m.entries = make([]entry[K, V], len(oldEntries)*2)

	for _, oldEntry := range oldEntries {
		if oldEntry.state != slotOccupied {
			continue
		}

		index, exists := m.findSlot(oldEntry.key)
		if exists {
			return errors.New("duplicate key during resize")
		}

		m.entries[index] = oldEntry
	}

	m.nonEmptySlots = m.occupiedSlots
	return nil
}

func (m *HashMap[K, V]) findSlot(key K) (uint64, bool) {
	index := m.index(key)
	perturb := key.Hash()

	var firstDeletedSlot uint64
	foundDeletedSlot := false

	for {
		switch m.entries[index].state {
		case slotEmpty:
			if foundDeletedSlot {
				return firstDeletedSlot, false
			}
			return index, false

		case slotOccupied:
			if m.entries[index].key == key {
				return index, true
			}

		case slotDeleted:
			if !foundDeletedSlot {
				firstDeletedSlot = index
				foundDeletedSlot = true
			}
		}

		index = m.nextIndex(index, &perturb)
	}
}

func (m *HashMap[K, V]) index(key K) uint64 {
	mask := uint64(len(m.entries) - 1)
	return key.Hash() & mask
}

func (m *HashMap[K, V]) nextIndex(index uint64, perturb *uint64) uint64 {
	*perturb >>= 5
	mask := uint64(len(m.entries) - 1)
	return (index*5 + *perturb + 1) & mask
}

func (m *HashMap[K, V]) hasSpace() bool {
	return m.nonEmptySlots < uint64(len(m.entries)-1)
}

func (m *HashMap[K, V]) validate() error {
	if m == nil || len(m.entries) == 0 {
		return errors.New("hashmap is not initialized")
	}
	return nil
}
