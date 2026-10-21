package hashmap

import (
	"errors"
	"slices"
	"testing"
)

type testKey struct {
	id   int
	hash uint64
}

// Hash returns the hash selected by a test, allowing deliberate collisions.
func (k testKey) Hash() uint64 {
	return k.hash
}

type testEntry struct {
	key   testKey
	value int
}

func checkHashMapContents(t *testing.T, m *HashMap[testKey, int], want map[testKey]int, phase string) {
	t.Helper()
	if got := m.Len(); got != uint64(len(want)) {
		t.Errorf("Len() %s = %d, want %d", phase, got, len(want))
	}
	for key, wantValue := range want {
		if got, exists := m.Get(key); !exists || got != wantValue {
			t.Errorf("Get(%#v) %s = (%d, %t), want (%d, true)", key, phase, got, exists, wantValue)
		}
	}
}

func checkHashMapMissing(t *testing.T, m *HashMap[testKey, int], keys []testKey, phase string) {
	t.Helper()
	for _, key := range keys {
		if got, exists := m.Get(key); exists || got != 0 {
			t.Errorf("Get(%#v) %s = (%d, %t), want (0, false)", key, phase, got, exists)
		}
	}
}

func checkHashMapList(t *testing.T, m *HashMap[testKey, int], want map[testKey]int, phase string) {
	t.Helper()
	got, err := m.ToList()
	if err != nil {
		t.Errorf("ToList() %s with entries %#v error = %v, want nil", phase, want, err)
		return
	}
	wantValues := make([]int, 0, len(want))
	for _, value := range want {
		wantValues = append(wantValues, value)
	}
	slices.Sort(got)
	slices.Sort(wantValues)
	if !slices.Equal(got, wantValues) {
		t.Errorf("ToList() %s with entries %#v = %v, want values %v in any order", phase, want, got, wantValues)
	}
	checkHashMapContents(t, m, want, phase)
}

// TestNewHashMap checks that construction creates an empty, usable table.
func TestNewHashMap(t *testing.T) {
	m := NewHashMap[testKey, int]()
	if m == nil {
		t.Errorf("NewHashMap[testKey, int]() = nil, want an initialized hashmap")
		return
	}
	checkHashMapContents(t, m, nil, "after construction")
	key := testKey{}
	checkHashMapMissing(t, m, []testKey{key}, "after construction")
	if err := m.Remove(key); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Remove(%#v) on an empty hashmap error = %v, want %v", key, err, ErrKeyNotFound)
	}
	checkHashMapContents(t, m, nil, "after rejecting removal from an empty hashmap")
}

// TestHashMapUninitialized checks every public operation on nil and zero values.
func TestHashMapUninitialized(t *testing.T) {
	tests := []struct {
		name string
		m    *HashMap[testKey, int]
	}{
		{name: "nil receiver"},
		{name: "zero value", m: &HashMap[testKey, int]{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := testKey{id: 1, hash: 42}
			if got := tt.m.Len(); got != 0 {
				t.Errorf("Len() on %s = %d, want 0", tt.name, got)
			}
			if err := tt.m.Insert(key, 10); !errors.Is(err, ErrUninitialized) {
				t.Errorf("Insert(%#v, 10) on %s error = %v, want %v", key, tt.name, err, ErrUninitialized)
			}
			if got, exists := tt.m.Get(key); exists || got != 0 {
				t.Errorf("Get(%#v) on %s = (%d, %t), want (0, false)", key, tt.name, got, exists)
			}
			if err := tt.m.Remove(key); !errors.Is(err, ErrUninitialized) {
				t.Errorf("Remove(%#v) on %s error = %v, want %v", key, tt.name, err, ErrUninitialized)
			}
			values, err := tt.m.ToList()
			if !errors.Is(err, ErrUninitialized) || len(values) != 0 {
				t.Errorf("ToList() on %s = %v, error = %v, want an empty slice and %v", tt.name, values, err, ErrUninitialized)
			}
			if got := tt.m.Len(); got != 0 {
				t.Errorf("Len() on %s after rejected operations = %d, want 0", tt.name, got)
			}
		})
	}
}

// TestHashMapInsertGet checks zero values, distinct keys, and collision probing.
func TestHashMapInsertGet(t *testing.T) {
	tests := []struct {
		name    string
		entries []testEntry
	}{
		{name: "empty"},
		{name: "zero key and value", entries: []testEntry{{key: testKey{}, value: 0}}},
		{name: "single entry", entries: []testEntry{{key: testKey{id: 1, hash: 42}, value: -10}}},
		{
			name: "distinct hashes",
			entries: []testEntry{
				{key: testKey{id: 1, hash: 1}, value: 10},
				{key: testKey{id: 2, hash: 2}, value: 10},
				{key: testKey{id: 3, hash: 3}, value: 30},
			},
		},
		{
			name: "zero hash collisions",
			entries: []testEntry{
				{key: testKey{id: 1}, value: 10},
				{key: testKey{id: 2}, value: 20},
				{key: testKey{id: 3}, value: 30},
			},
		},
		{
			name: "maximum hash collisions",
			entries: []testEntry{
				{key: testKey{id: 1, hash: ^uint64(0)}, value: 10},
				{key: testKey{id: 2, hash: ^uint64(0)}, value: 20},
				{key: testKey{id: 3, hash: ^uint64(0)}, value: 30},
			},
		},
		{
			name: "same low bits with different high bits",
			entries: []testEntry{
				{key: testKey{id: 1, hash: 1}, value: 10},
				{key: testKey{id: 2, hash: 1 | 1<<32}, value: 20},
				{key: testKey{id: 3, hash: 1 | 1<<63}, value: 30},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHashMap[testKey, int]()
			want := make(map[testKey]int, len(tt.entries))
			for _, entry := range tt.entries {
				if err := m.Insert(entry.key, entry.value); err != nil {
					t.Errorf("Insert(%#v, %d) error = %v, want nil", entry.key, entry.value, err)
					return
				}
				want[entry.key] = entry.value
				if got := m.Len(); got != uint64(len(want)) {
					t.Errorf("Len() after Insert(%#v, %d) = %d, want %d", entry.key, entry.value, got, len(want))
				}
			}
			checkHashMapContents(t, m, want, "after insertions")
			missing := testKey{id: -1}
			if len(tt.entries) != 0 {
				missing.hash = tt.entries[0].key.hash
			}
			checkHashMapMissing(t, m, []testKey{missing}, "after insertions")
		})
	}
}

// TestHashMapDuplicateKey checks that rejected duplicates preserve stored values.
func TestHashMapDuplicateKey(t *testing.T) {
	tests := []struct {
		name        string
		count       int
		removeFirst bool
	}{
		{name: "single entry", count: 1},
		{name: "before growth", count: 7},
		{name: "after growth", count: 32},
		{name: "after a deleted collision", count: 4, removeFirst: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHashMap[testKey, int]()
			want := make(map[testKey]int, tt.count)
			for id := 0; id < tt.count; id++ {
				key := testKey{id: id}
				value := id + 10
				if err := m.Insert(key, value); err != nil {
					t.Errorf("Insert(%#v, %d) during setup error = %v, want nil", key, value, err)
					return
				}
				want[key] = value
			}
			if tt.removeFirst {
				key := testKey{}
				if err := m.Remove(key); err != nil {
					t.Errorf("Remove(%#v) during setup error = %v, want nil", key, err)
					return
				}
				delete(want, key)
			}
			duplicate := testKey{id: tt.count - 1}
			if err := m.Insert(duplicate, -1); !errors.Is(err, ErrKeyExists) {
				t.Errorf("Insert(%#v, -1) with an existing key error = %v, want %v", duplicate, err, ErrKeyExists)
			}
			checkHashMapContents(t, m, want, "after rejecting a duplicate")
		})
	}
}

// TestHashMapRemove checks missing keys, collision chains, and deleted-slot reuse.
func TestHashMapRemove(t *testing.T) {
	tests := []struct {
		name     string
		count    int
		removeID int
		hash     uint64
	}{
		{name: "empty"},
		{name: "missing key", count: 3, removeID: 99},
		{name: "only entry", count: 1},
		{name: "first collision", count: 3},
		{name: "middle collision", count: 3, removeID: 1},
		{name: "last collision", count: 3, removeID: 2},
		{name: "long collision chain", count: 64, removeID: 30, hash: ^uint64(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHashMap[testKey, int]()
			want := make(map[testKey]int, tt.count)
			for id := 0; id < tt.count; id++ {
				key := testKey{id: id, hash: tt.hash}
				value := id + 10
				if err := m.Insert(key, value); err != nil {
					t.Errorf("Insert(%#v, %d) during setup error = %v, want nil", key, value, err)
					return
				}
				want[key] = value
			}
			key := testKey{id: tt.removeID, hash: tt.hash}
			_, wasPresent := want[key]
			err := m.Remove(key)
			var wantErr error
			if !wasPresent {
				wantErr = ErrKeyNotFound
			}
			if !errors.Is(err, wantErr) {
				t.Errorf("Remove(%#v) with key present = %t error = %v, want %v", key, wasPresent, err, wantErr)
			}
			delete(want, key)
			checkHashMapContents(t, m, want, "after removal")
			checkHashMapMissing(t, m, []testKey{key}, "after removal")
			if err := m.Remove(key); !errors.Is(err, ErrKeyNotFound) {
				t.Errorf("Remove(%#v) a second time error = %v, want %v", key, err, ErrKeyNotFound)
			}
			checkHashMapContents(t, m, want, "after rejecting repeated removal")

			replacement := testKey{id: tt.count + 100, hash: tt.hash}
			if err := m.Insert(replacement, 77); err != nil {
				t.Errorf("Insert(%#v, 77) after removal error = %v, want nil", replacement, err)
				return
			}
			want[replacement] = 77
			checkHashMapContents(t, m, want, "after inserting a new colliding key")
			checkHashMapMissing(t, m, []testKey{key}, "after inserting a new colliding key")
			if err := m.Insert(key, -1); err != nil {
				t.Errorf("Insert(%#v, -1) after removal error = %v, want nil", key, err)
				return
			}
			want[key] = -1
			checkHashMapContents(t, m, want, "after reinsertion")
		})
	}
}

// TestHashMapNilValue distinguishes a stored nil pointer from a missing key.
func TestHashMapNilValue(t *testing.T) {
	m := NewHashMap[testKey, *int]()
	key := testKey{}
	if err := m.Insert(key, nil); err != nil {
		t.Errorf("Insert(%#v, nil) error = %v, want nil", key, err)
		return
	}
	if got, exists := m.Get(key); got != nil || !exists {
		t.Errorf("Get(%#v) after Insert with nil = (%v, %t), want (nil, true)", key, got, exists)
	}
	missing := testKey{id: 1}
	if got, exists := m.Get(missing); got != nil || exists {
		t.Errorf("Get(%#v) for a missing key = (%v, %t), want (nil, false)", missing, got, exists)
	}
	if got := m.Len(); got != 1 {
		t.Errorf("Len() after Insert(%#v, nil) = %d, want 1", key, got)
	}
	if err := m.Remove(key); err != nil {
		t.Errorf("Remove(%#v) with a stored nil value error = %v, want nil", key, err)
	}
	if got, exists := m.Get(key); got != nil || exists {
		t.Errorf("Get(%#v) after removal = (%v, %t), want (nil, false)", key, got, exists)
	}
	if got := m.Len(); got != 0 {
		t.Errorf("Len() after Remove(%#v) = %d, want 0", key, got)
	}
}

// TestHashMapToList checks empty, minimal, colliding, and large sets of values.
func TestHashMapToList(t *testing.T) {
	large := make([]testEntry, 1024)
	for id := range large {
		large[id] = testEntry{key: testKey{id: id, hash: uint64(id)}, value: id % 17}
	}
	tests := []struct {
		name    string
		entries []testEntry
	}{
		{name: "empty"},
		{name: "zero key and value", entries: []testEntry{{key: testKey{}, value: 0}}},
		{name: "single entry", entries: []testEntry{{key: testKey{id: 1, hash: 42}, value: -10}}},
		{
			name: "equal values under distinct keys",
			entries: []testEntry{
				{key: testKey{id: 1, hash: 1}, value: 7},
				{key: testKey{id: 2, hash: 2}, value: 7},
				{key: testKey{id: 3, hash: 3}, value: 0},
			},
		},
		{
			name: "zero hash collisions",
			entries: []testEntry{
				{key: testKey{id: 1}, value: 30},
				{key: testKey{id: 2}, value: -10},
				{key: testKey{id: 3}, value: 20},
			},
		},
		{
			name: "maximum hash collisions",
			entries: []testEntry{
				{key: testKey{id: 1, hash: ^uint64(0)}, value: 30},
				{key: testKey{id: 2, hash: ^uint64(0)}, value: 10},
				{key: testKey{id: 3, hash: ^uint64(0)}, value: 20},
			},
		},
		{name: "large input", entries: large},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHashMap[testKey, int]()
			want := make(map[testKey]int, len(tt.entries))
			for _, entry := range tt.entries {
				if err := m.Insert(entry.key, entry.value); err != nil {
					t.Errorf("Insert(%#v, %d) during setup error = %v, want nil", entry.key, entry.value, err)
					return
				}
				want[entry.key] = entry.value
			}
			checkHashMapList(t, m, want, "after insertion")
		})
	}
}

// TestHashMapToListAfterChanges checks deleted slots, reinsertion, growth, and shrinking.
func TestHashMapToListAfterChanges(t *testing.T) {
	tests := []struct {
		name  string
		count int
		hash  uint64
	}{
		{name: "minimal", count: 1},
		{name: "deleted collision", count: 4},
		{name: "growth and shrinking", count: 128},
		{name: "maximum hash collisions", count: 64, hash: ^uint64(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHashMap[testKey, int]()
			want := make(map[testKey]int, tt.count)
			for id := 0; id < tt.count; id++ {
				key := testKey{id: id, hash: tt.hash}
				if err := m.Insert(key, id); err != nil {
					t.Errorf("Insert(%#v, %d) during setup error = %v, want nil", key, id, err)
					return
				}
				want[key] = id
			}
			checkHashMapList(t, m, want, "after insertion")

			key := testKey{id: tt.count / 2, hash: tt.hash}
			if err := m.Remove(key); err != nil {
				t.Errorf("Remove(%#v) with %d entries error = %v, want nil", key, tt.count, err)
				return
			}
			delete(want, key)
			checkHashMapList(t, m, want, "after removing a colliding key")
			if err := m.Insert(key, -1); err != nil {
				t.Errorf("Insert(%#v, -1) after removal error = %v, want nil", key, err)
				return
			}
			want[key] = -1
			checkHashMapList(t, m, want, "after reinsertion with a new value")

			for id := 0; id < tt.count; id++ {
				key := testKey{id: id, hash: tt.hash}
				if err := m.Remove(key); err != nil {
					t.Errorf("Remove(%#v) while emptying %d entries error = %v, want nil", key, tt.count, err)
					return
				}
				delete(want, key)
				checkHashMapList(t, m, want, "while shrinking and emptying the hashmap")
			}
		})
	}
}

// TestHashMapToListCopyIsolation checks independence from the hashmap and other results.
func TestHashMapToListCopyIsolation(t *testing.T) {
	m := NewHashMap[testKey, int]()
	key := testKey{id: 1}
	if err := m.Insert(key, 10); err != nil {
		t.Errorf("Insert(%#v, 10) during setup error = %v, want nil", key, err)
		return
	}
	first, err := m.ToList()
	if err != nil || !slices.Equal(first, []int{10}) {
		t.Errorf("ToList() with entries {%#v: 10} = %v, error = %v, want [10] and nil", key, first, err)
		return
	}
	second, err := m.ToList()
	if err != nil || !slices.Equal(second, []int{10}) {
		t.Errorf("ToList() a second time with entries {%#v: 10} = %v, error = %v, want [10] and nil", key, second, err)
		return
	}
	first[0] = -1
	if got, exists := m.Get(key); !exists || got != 10 {
		t.Errorf("Get(%#v) after modifying the first ToList() result = (%d, %t), want (10, true)", key, got, exists)
	}
	if second[0] != 10 {
		t.Errorf("second ToList() result after setting the first result to [-1] = %v, want [10]", second)
	}
	if err := m.Remove(key); err != nil {
		t.Errorf("Remove(%#v) after ToList() error = %v, want nil", key, err)
		return
	}
	if err := m.Insert(key, 20); err != nil {
		t.Errorf("Insert(%#v, 20) after ToList() error = %v, want nil", key, err)
		return
	}
	if !slices.Equal(second, []int{10}) {
		t.Errorf("ToList() result after replacing value at key %#v with 20 = %v, want [10]", key, second)
	}
	checkHashMapList(t, m, map[testKey]int{key: 20}, "after replacing a value")
}

// TestHashMapToListPointerValues checks nil values, repeated pointers, and shallow copies.
func TestHashMapToListPointerValues(t *testing.T) {
	m := NewHashMap[testKey, *int]()
	value := 10
	values := []*int{nil, &value, &value}
	for id, pointer := range values {
		key := testKey{id: id}
		if err := m.Insert(key, pointer); err != nil {
			t.Errorf("Insert(%#v, %p) during setup error = %v, want nil", key, pointer, err)
			return
		}
	}
	got, err := m.ToList()
	if err != nil {
		t.Errorf("ToList() with values %v error = %v, want nil", values, err)
		return
	}
	wantCounts := map[*int]int{nil: 1, &value: 2}
	counts := make(map[*int]int)
	for _, pointer := range got {
		counts[pointer]++
	}
	if len(got) != len(values) || len(counts) != len(wantCounts) {
		t.Errorf("ToList() with values %v = %v with counts %v, want counts %v", values, got, counts, wantCounts)
	}
	for pointer, wantCount := range wantCounts {
		if counts[pointer] != wantCount {
			t.Errorf("ToList() with values %v contains pointer %p %d times, want %d", values, pointer, counts[pointer], wantCount)
		}
	}
}

// TestHashMapGrowth checks retained entries and counts across growth and deletions.
func TestHashMapGrowth(t *testing.T) {
	identityHash := func(id int) uint64 { return uint64(id) }
	tests := []struct {
		name  string
		count int
		hash  func(int) uint64
	}{
		{name: "empty", hash: identityHash},
		{name: "single entry", count: 1, hash: identityHash},
		{name: "large input", count: 4096, hash: identityHash},
		{name: "zero hash collisions", count: 1024, hash: func(int) uint64 { return 0 }},
		{name: "maximum hash collisions", count: 1024, hash: func(int) uint64 { return ^uint64(0) }},
		{name: "common low bits", count: 2048, hash: func(id int) uint64 { return uint64(id) << 32 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHashMap[testKey, int]()
			want := make(map[testKey]int, tt.count*2)
			for id := 0; id < tt.count; id++ {
				key := testKey{id: id, hash: tt.hash(id)}
				value := id*3 + 1
				if err := m.Insert(key, value); err != nil {
					t.Errorf("Insert(%#v, %d) during growth error = %v, want nil", key, value, err)
					return
				}
				want[key] = value
			}
			checkHashMapContents(t, m, want, "after initial insertions")

			var removed []testKey
			for id := 0; id < tt.count; id += 2 {
				key := testKey{id: id, hash: tt.hash(id)}
				if err := m.Remove(key); err != nil {
					t.Errorf("Remove(%#v) after growth error = %v, want nil", key, err)
				}
				delete(want, key)
				removed = append(removed, key)
			}
			checkHashMapContents(t, m, want, "after removing alternating keys")
			checkHashMapMissing(t, m, removed, "after removing alternating keys")

			for id := tt.count; id < tt.count*2; id++ {
				key := testKey{id: id, hash: tt.hash(id)}
				value := id*3 + 1
				if err := m.Insert(key, value); err != nil {
					t.Errorf("Insert(%#v, %d) after deletions error = %v, want nil", key, value, err)
					return
				}
				want[key] = value
			}
			checkHashMapContents(t, m, want, "after growth with deleted slots")
			checkHashMapMissing(t, m, removed, "after growth with deleted slots")

			for _, key := range removed {
				value := -key.id - 1
				if err := m.Insert(key, value); err != nil {
					t.Errorf("Insert(%#v, %d) during reinsertion error = %v, want nil", key, value, err)
					return
				}
				want[key] = value
			}
			checkHashMapContents(t, m, want, "after reinserting deleted keys with new values")

			for id := 0; id < tt.count*2; id++ {
				key := testKey{id: id, hash: tt.hash(id)}
				if err := m.Remove(key); err != nil {
					t.Errorf("Remove(%#v) while emptying the hashmap error = %v, want nil", key, err)
				}
				delete(want, key)
				if got := m.Len(); got != uint64(len(want)) {
					t.Errorf("Len() after Remove(%#v) while emptying the hashmap = %d, want %d", key, got, len(want))
				}
			}
			checkHashMapContents(t, m, want, "after removing every key")
			checkHashMapMissing(t, m, removed, "after removing every key")
		})
	}
}

func newDuplicateTable() *HashMap[testKey, int] {
	duplicate := &HashMap[testKey, int]{
		entries: make([]entry[testKey, int], initialCapacity*2),
	}
	key := testKey{id: 1}
	// Deliberately corrupt the table to exercise the internal duplicate guard.
	duplicate.entries[0] = entry[testKey, int]{key: key, value: 10, state: slotOccupied}
	duplicate.entries[1] = entry[testKey, int]{key: key, value: 20, state: slotOccupied}
	duplicate.entries[2] = entry[testKey, int]{key: testKey{id: 2, hash: 2}, value: 30, state: slotOccupied}
	duplicate.entries[3] = entry[testKey, int]{state: slotDeleted}
	duplicate.occupiedSlots = 3
	duplicate.nonEmptySlots = 4
	return duplicate
}

// TestHashMapInsertGrowError checks error propagation and state preservation after failed growth.
func TestHashMapInsertGrowError(t *testing.T) {
	m := newDuplicateTable()
	// Leave one empty slot while forcing Insert to grow before probing.
	for index := 0; index < len(m.entries)-1; index++ {
		if m.entries[index].state == slotEmpty {
			m.entries[index] = entry[testKey, int]{state: slotDeleted}
			m.nonEmptySlots++
		}
	}
	key := testKey{id: 3, hash: 3}
	wantEntries := slices.Clone(m.entries)
	wantCapacity := cap(m.entries)
	wantOccupiedSlots := m.occupiedSlots
	wantNonEmptySlots := m.nonEmptySlots

	if err := m.Insert(key, 40); !errors.Is(err, errDuplicateKeyDuringResize) {
		t.Errorf("Insert(%#v, 40) with duplicate entries and no insertion space error = %v, want %v", key, err, errDuplicateKeyDuringResize)
	}
	if !slices.Equal(m.entries, wantEntries) {
		t.Errorf("Insert(%#v, 40) after failed growth entries = %#v, want %#v", key, m.entries, wantEntries)
	}
	if got := cap(m.entries); got != wantCapacity {
		t.Errorf("Insert(%#v, 40) after failed growth capacity = %d, want %d", key, got, wantCapacity)
	}
	if got := m.occupiedSlots; got != wantOccupiedSlots {
		t.Errorf("Insert(%#v, 40) after failed growth occupied slots = %d, want %d", key, got, wantOccupiedSlots)
	}
	if got := m.nonEmptySlots; got != wantNonEmptySlots {
		t.Errorf("Insert(%#v, 40) after failed growth non-empty slots = %d, want %d", key, got, wantNonEmptySlots)
	}
	checkHashMapMissing(t, m, []testKey{key}, "after failed growth in Insert")
	storedKey := testKey{id: 2, hash: 2}
	if got, exists := m.Get(storedKey); !exists || got != 30 {
		t.Errorf("Get(%#v) after failed growth in Insert = (%d, %t), want (30, true)", storedKey, got, exists)
	}
}

// TestHashMapResizeErrors checks validation and duplicate detection during rehashing.
// Failed resizes must preserve entries, capacity, and slot counts.
func TestHashMapResizeErrors(t *testing.T) {
	tests := []struct {
		name    string
		m       *HashMap[testKey, int]
		run     func(*HashMap[testKey, int]) error
		wantErr error
	}{
		{name: "resize on nil receiver", run: (*HashMap[testKey, int]).grow, wantErr: ErrUninitialized},
		{name: "resize on zero value", m: &HashMap[testKey, int]{}, run: (*HashMap[testKey, int]).grow, wantErr: ErrUninitialized},
		{name: "resize on duplicate entries", m: newDuplicateTable(), run: (*HashMap[testKey, int]).grow, wantErr: errDuplicateKeyDuringResize},
		{name: "shrink on nil receiver", run: (*HashMap[testKey, int]).shrink, wantErr: ErrUninitialized},
		{name: "shrink on zero value", m: &HashMap[testKey, int]{}, run: (*HashMap[testKey, int]).shrink, wantErr: ErrUninitialized},
		{name: "shrink on duplicate entries", m: newDuplicateTable(), run: (*HashMap[testKey, int]).shrink, wantErr: errDuplicateKeyDuringResize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wantEntries []entry[testKey, int]
			var wantCapacity int
			var wantOccupiedSlots, wantNonEmptySlots uint64
			if tt.m != nil {
				wantEntries = slices.Clone(tt.m.entries)
				wantCapacity = cap(tt.m.entries)
				wantOccupiedSlots = tt.m.occupiedSlots
				wantNonEmptySlots = tt.m.nonEmptySlots
			}

			if err := tt.run(tt.m); !errors.Is(err, tt.wantErr) {
				t.Errorf("%s error = %v, want %v", tt.name, err, tt.wantErr)
			}
			if tt.m == nil {
				return
			}
			if !slices.Equal(tt.m.entries, wantEntries) {
				t.Errorf("%s entries = %#v, want %#v", tt.name, tt.m.entries, wantEntries)
			}
			if got := cap(tt.m.entries); got != wantCapacity {
				t.Errorf("%s capacity = %d, want %d", tt.name, got, wantCapacity)
			}
			if got := tt.m.occupiedSlots; got != wantOccupiedSlots {
				t.Errorf("%s occupied slots = %d, want %d", tt.name, got, wantOccupiedSlots)
			}
			if got := tt.m.nonEmptySlots; got != wantNonEmptySlots {
				t.Errorf("%s non-empty slots = %d, want %d", tt.name, got, wantNonEmptySlots)
			}
		})
	}
}

// TestHashMapShrink checks the removal threshold, retained collisions, and minimum capacity.
func TestHashMapShrink(t *testing.T) {
	tests := []struct {
		name         string
		capacity     int
		count        int
		removeID     int
		hash         uint64
		wantCapacity int
		wantErr      error
	}{
		{name: "empty", capacity: 8, wantCapacity: 8, wantErr: ErrKeyNotFound},
		{name: "missing key in a sparse table", capacity: 16, count: 1, removeID: 99, wantCapacity: 16, wantErr: ErrKeyNotFound},
		{name: "minimum capacity", capacity: 8, count: 1, wantCapacity: 8},
		{name: "above threshold", capacity: 16, count: 6, wantCapacity: 16},
		{name: "at threshold", capacity: 16, count: 5, wantCapacity: 16},
		{name: "below threshold", capacity: 16, count: 4, wantCapacity: 8},
		{name: "empty after removal", capacity: 16, count: 1, wantCapacity: 8},
		{name: "zero hash collisions", capacity: 32, count: 8, removeID: 3, wantCapacity: 16},
		{name: "maximum hash collisions", capacity: 64, count: 16, removeID: 7, hash: ^uint64(0), wantCapacity: 32},
		{name: "large input", capacity: 4096, count: 1024, removeID: 512, wantCapacity: 2048},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &HashMap[testKey, int]{
				entries: make([]entry[testKey, int], tt.capacity),
			}
			want := make(map[testKey]int, tt.count)
			for id := 0; id < tt.count; id++ {
				key := testKey{id: id, hash: tt.hash}
				value := id + 10
				if err := m.Insert(key, value); err != nil {
					t.Errorf("Insert(%#v, %d) during setup error = %v, want nil", key, value, err)
					return
				}
				want[key] = value
			}
			key := testKey{id: tt.removeID, hash: tt.hash}
			if err := m.Remove(key); !errors.Is(err, tt.wantErr) {
				t.Errorf("Remove(%#v) with capacity %d and %d entries error = %v, want %v", key, tt.capacity, tt.count, err, tt.wantErr)
				return
			}
			delete(want, key)
			if got := len(m.entries); got != tt.wantCapacity {
				t.Errorf("Remove(%#v) with capacity %d and %d entries capacity = %d, want %d", key, tt.capacity, tt.count, got, tt.wantCapacity)
			}
			checkHashMapContents(t, m, want, "after removal with automatic shrinking")
			checkHashMapMissing(t, m, []testKey{key}, "after removal with automatic shrinking")
			if tt.wantCapacity < tt.capacity {
				if m.nonEmptySlots != m.occupiedSlots {
					t.Errorf("Remove(%#v) after shrinking non-empty slots = %d, want %d", key, m.nonEmptySlots, m.occupiedSlots)
				}
				for index, entry := range m.entries {
					if entry.state == slotDeleted {
						t.Errorf("Remove(%#v) after shrinking slot %d state = %d, want a non-deleted slot", key, index, entry.state)
					}
				}
			}
		})
	}
}

// TestHashMapRemoveShrinkError checks that a failed shrink also rolls back removal.
func TestHashMapRemoveShrinkError(t *testing.T) {
	m := newDuplicateTable()
	key := testKey{id: 2, hash: 2}
	wantEntries := slices.Clone(m.entries)
	wantCapacity := cap(m.entries)
	wantOccupiedSlots := m.occupiedSlots
	wantNonEmptySlots := m.nonEmptySlots

	if err := m.Remove(key); !errors.Is(err, errDuplicateKeyDuringResize) {
		t.Errorf("Remove(%#v) with duplicate entries error = %v, want %v", key, err, errDuplicateKeyDuringResize)
	}
	if !slices.Equal(m.entries, wantEntries) {
		t.Errorf("Remove(%#v) after failed shrinking entries = %#v, want %#v", key, m.entries, wantEntries)
	}
	if got := cap(m.entries); got != wantCapacity {
		t.Errorf("Remove(%#v) after failed shrinking capacity = %d, want %d", key, got, wantCapacity)
	}
	if got := m.occupiedSlots; got != wantOccupiedSlots {
		t.Errorf("Remove(%#v) after failed shrinking occupied slots = %d, want %d", key, got, wantOccupiedSlots)
	}
	if got := m.nonEmptySlots; got != wantNonEmptySlots {
		t.Errorf("Remove(%#v) after failed shrinking non-empty slots = %d, want %d", key, got, wantNonEmptySlots)
	}
	if got, exists := m.Get(key); !exists || got != 30 {
		t.Errorf("Get(%#v) after failed removal = (%d, %t), want (30, true)", key, got, exists)
	}
}

// TestHashMapGrowShrinkCycles checks repeated growth and shrinking with reusable keys.
func TestHashMapGrowShrinkCycles(t *testing.T) {
	tests := []struct {
		name  string
		count int
		hash  uint64
	}{
		{name: "empty"},
		{name: "single entry", count: 1},
		{name: "large input", count: 256},
		{name: "maximum hash collisions", count: 128, hash: ^uint64(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewHashMap[testKey, int]()
			wantCapacity := initialCapacity
			for wantCapacity <= tt.count {
				wantCapacity *= 2
			}
			for cycle := 0; cycle < 3; cycle++ {
				want := make(map[testKey]int, tt.count)
				keys := make([]testKey, 0, tt.count)
				for id := 0; id < tt.count; id++ {
					key := testKey{id: id, hash: tt.hash}
					value := cycle*tt.count + id + 1
					if err := m.Insert(key, value); err != nil {
						t.Errorf("Insert(%#v, %d) during cycle %d error = %v, want nil", key, value, cycle, err)
						return
					}
					want[key] = value
					keys = append(keys, key)
				}
				if got := len(m.entries); got != wantCapacity {
					t.Errorf("Insert() with %d entries during cycle %d capacity = %d, want %d", tt.count, cycle, got, wantCapacity)
				}
				checkHashMapContents(t, m, want, "after growing during a cycle")
				for _, key := range keys {
					if err := m.Remove(key); err != nil {
						t.Errorf("Remove(%#v) during cycle %d error = %v, want nil", key, cycle, err)
						return
					}
					delete(want, key)
					checkHashMapContents(t, m, want, "while shrinking during a cycle")
				}
				checkHashMapContents(t, m, want, "after emptying during a cycle")
				checkHashMapMissing(t, m, keys, "after emptying during a cycle")
				if got := len(m.entries); got != initialCapacity {
					t.Errorf("Remove() with %d entries during cycle %d final capacity = %d, want %d", tt.count, cycle, got, initialCapacity)
				}
			}
		})
	}
}
