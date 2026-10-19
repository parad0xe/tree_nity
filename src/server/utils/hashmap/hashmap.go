package hashmap

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
// It returns ErrKeyExists if key already exists, or ErrUninitialized if m
// is nil or uninitialized.
func (m *HashMap[K, V]) Insert(key K, value V) error {
	if err := m.validate(); err != nil {
		return err
	}

	if !m.hasSpace() {
		if err := m.grow(); err != nil {
			return err
		}
	}

	index, exists := m.findSlot(key)
	if exists {
		return ErrKeyExists
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
// It returns ErrKeyNotFound if key is absent, or ErrUninitialized if m
// is nil or uninitialized.
// It may halve the table below 25% occupancy, keeping at least its initial capacity.
// If rebuilding fails, the removal is rolled back and the error is returned.
func (m *HashMap[K, V]) Remove(key K) error {
	if err := m.validate(); err != nil {
		return err
	}

	index, exists := m.findSlot(key)
	if !exists {
		return ErrKeyNotFound
	}

	removedEntry := m.entries[index]
	m.entries[index] = entry[K, V]{state: slotDeleted}
	m.occupiedSlots--
	if err := m.shrink(); err != nil {
		m.entries[index] = removedEntry
		m.occupiedSlots++
		return err
	}
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

func (m *HashMap[K, V]) grow() error {
	if err := m.validate(); err != nil {
		return err
	}

	return m.rehash(len(m.entries) * 2)
}

func (m *HashMap[K, V]) shrink() error {
	if err := m.validate(); err != nil {
		return err
	}

	if len(m.entries) <= initialCapacity || m.occupiedSlots >= uint64(len(m.entries)/4) {
		return nil
	}
	return m.rehash(len(m.entries) / 2)
}

func (m *HashMap[K, V]) rehash(capacity int) error {
	tmpTable := HashMap[K, V]{
		entries: make([]entry[K, V], capacity),
	}

	for _, oldEntry := range m.entries {
		if oldEntry.state != slotOccupied {
			continue
		}

		index, exists := tmpTable.findSlot(oldEntry.key)
		if exists {
			return errDuplicateKeyDuringResize
		}

		tmpTable.entries[index] = oldEntry
	}

	m.entries = tmpTable.entries
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
		return ErrUninitialized
	}
	return nil
}
