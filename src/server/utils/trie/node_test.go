package trie

import (
	"slices"
	"testing"
)

// TestNewNode checks that a new node is empty and ready to accept children.
func TestNewNode(t *testing.T) {
	node := NewNode[int]()
	if node == nil {
		t.Errorf("NewNode[int]() = nil, want an initialized node")
		return
	}
	if node.children == nil {
		t.Errorf("NewNode[int]().children = nil, want an initialized map")
	}
	if !node.IsEmpty() {
		t.Errorf("NewNode[int]().IsEmpty() = false, want true (values=%v, children=%v)", node.values, node.children)
	}
}

// TestNodeEnsureChild checks creation, reuse, and isolation of byte-keyed children.
func TestNodeEnsureChild(t *testing.T) {
	tests := []struct {
		name string
		key  byte
	}{
		{name: "zero byte", key: 0},
		{name: "ASCII byte", key: 'a'},
		{name: "maximum byte", key: 255},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewNode[int]()
			child := node.EnsureChild(tt.key)
			if child == nil {
				t.Errorf("EnsureChild(%d) = nil, want an initialized child", tt.key)
				return
			}
			if err := child.AddValue(7); err != nil {
				t.Errorf("AddValue(%d) on child %d error = %v, want nil", 7, tt.key, err)
				return
			}
			if got := node.EnsureChild(tt.key); got != child {
				t.Errorf("EnsureChild(%d) on an existing child = %p, want %p", tt.key, got, child)
			}
			if !slices.Equal(child.values, []int{7}) {
				t.Errorf("EnsureChild(%d) preserved values = %v, want [7]", tt.key, child.values)
			}

			siblingKey := tt.key ^ 255
			sibling := node.EnsureChild(siblingKey)
			if sibling == nil || sibling == child {
				t.Errorf("EnsureChild(%d) = %p, want a non-nil child distinct from %p", siblingKey, sibling, child)
			}
			if got := node.children[tt.key]; got != child {
				t.Errorf("EnsureChild(%d) changed child at %d to %p, want %p", siblingKey, tt.key, got, child)
			}
			if len(node.children) != 2 {
				t.Errorf("EnsureChild(%d) then EnsureChild(%d) child count = %d, want 2", tt.key, siblingKey, len(node.children))
			}
		})
	}
}

// TestNodeAddValue checks duplicate rejection and the values retained after each call.
func TestNodeAddValue(t *testing.T) {
	large := make([]int, 1024)
	for i := range large {
		large[i] = i
	}
	tests := []struct {
		name      string
		values    []int
		want      []int
		wantErrAt map[int]bool
	}{
		{name: "empty"},
		{name: "zero value", values: []int{0}, want: []int{0}},
		{name: "duplicate zero", values: []int{0, 0}, want: []int{0}, wantErrAt: map[int]bool{1: true}},
		{name: "duplicates", values: []int{1, 1, 2, 1, 2}, want: []int{1, 2}, wantErrAt: map[int]bool{1: true, 3: true, 4: true}},
		{name: "large", values: large, want: large},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewNode[int]()
			for i, value := range tt.values {
				before := slices.Clone(node.values)
				err := node.AddValue(value)
				if tt.wantErrAt[i] {
					if err == nil || err.Error() != "value already exists" {
						t.Errorf("AddValue(%d) at index %d in %v error = %v, want %q", value, i, tt.values, err, "value already exists")
					}
					if !slices.Equal(node.values, before) {
						t.Errorf("AddValue(%d) at index %d in %v changed values to %v, want unchanged values %v", value, i, tt.values, node.values, before)
					}
				} else if err != nil {
					t.Errorf("AddValue(%d) at index %d in %v error = %v, want nil", value, i, tt.values, err)
				}
			}
			if !slices.Equal(node.values, tt.want) {
				t.Errorf("AddValue sequence %v stored %v, want %v", tt.values, node.values, tt.want)
			}
		})
	}
}

// TestNodeRemoveValue checks removal positions, missing values, and rejected duplicates.
func TestNodeRemoveValue(t *testing.T) {
	tests := []struct {
		name      string
		values    []int
		value     int
		want      []int
		found     bool
		wantErrAt map[int]bool
	}{
		{name: "empty"},
		{name: "missing", values: []int{1, 2, 3}, value: 4, want: []int{1, 2, 3}},
		{name: "first", values: []int{1, 2, 3}, value: 1, want: []int{2, 3}, found: true},
		{name: "middle", values: []int{1, 2, 3}, value: 2, want: []int{1, 3}, found: true},
		{name: "last", values: []int{1, 2, 3}, value: 3, want: []int{1, 2}, found: true},
		{name: "only value", values: []int{1}, value: 1, found: true},
		{name: "zero value", values: []int{0}, value: 0, found: true},
		{name: "after rejected duplicate", values: []int{1, 1, 2}, value: 1, want: []int{2}, found: true, wantErrAt: map[int]bool{1: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewNode[int]()
			for i, value := range tt.values {
				err := node.AddValue(value)
				if tt.wantErrAt[i] {
					if err == nil || err.Error() != "value already exists" {
						t.Errorf("AddValue(%d) at setup index %d in %v error = %v, want %q", value, i, tt.values, err, "value already exists")
					}
				} else if err != nil {
					t.Errorf("AddValue(%d) at setup index %d in %v error = %v, want nil", value, i, tt.values, err)
					return
				}
			}
			if got := node.RemoveValue(tt.value); got != tt.found {
				t.Errorf("RemoveValue(%d) with values %v = %t, want %t", tt.value, tt.values, got, tt.found)
			}
			if !slices.Equal(node.values, tt.want) {
				t.Errorf("RemoveValue(%d) with values %v left %v, want %v", tt.value, tt.values, node.values, tt.want)
			}
		})
	}
}

// TestNodeRemoveValuePointerIdentity checks equality for comparable pointer values.
func TestNodeRemoveValuePointerIdentity(t *testing.T) {
	type value struct {
		id int
	}
	first := &value{id: 1}
	second := &value{id: 1}
	missing := &value{id: 1}
	node := NewNode[*value]()
	for _, pointer := range []*value{first, second} {
		if err := node.AddValue(pointer); err != nil {
			t.Errorf("AddValue(%p) during setup error = %v, want nil", pointer, err)
			return
		}
	}
	if err := node.AddValue(first); err == nil || err.Error() != "value already exists" {
		t.Errorf("AddValue(%p) with values [%p %p] error = %v, want %q", first, first, second, err, "value already exists")
	}

	if got := node.RemoveValue(missing); got {
		t.Errorf("RemoveValue(%p) with values [%p %p] = true, want false", missing, first, second)
	}
	if !slices.Equal(node.values, []*value{first, second}) {
		t.Errorf("RemoveValue(%p) left %v, want [%p %p]", missing, node.values, first, second)
	}
	if got := node.RemoveValue(second); !got {
		t.Errorf("RemoveValue(%p) with values [%p %p] = false, want true", second, first, second)
	}
	if !slices.Equal(node.values, []*value{first}) {
		t.Errorf("RemoveValue(%p) left %v, want [%p]", second, node.values, first)
	}
}

// TestNodeMapSubtree checks that equal values at different nodes are all visited.
func TestNodeMapSubtree(t *testing.T) {
	tests := []struct {
		name    string
		entries map[string][]int
		want    []int
	}{
		{name: "empty"},
		{name: "single value", entries: map[string][]int{"": {0}}, want: []int{0}},
		{name: "descendants only", entries: map[string][]int{"a": {1}, "ab": {2}, "z": {3}}, want: []int{1, 2, 3}},
		{name: "equal values at different nodes", entries: map[string][]int{"": {1}, "a": {1, 2}, "ab": {3}, "z": {4}}, want: []int{1, 1, 2, 3, 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := NewNode[int]()
			for prefix, values := range tt.entries {
				node := root
				for i := 0; i < len(prefix); i++ {
					node = node.EnsureChild(prefix[i])
				}
				for _, value := range values {
					if err := node.AddValue(value); err != nil {
						t.Errorf("AddValue(%d) at prefix %q during setup error = %v, want nil", value, prefix, err)
						return
					}
				}
			}

			var got []int
			root.MapSubtree(func(value int) {
				got = append(got, value)
			})
			slices.Sort(got)
			if !slices.Equal(got, tt.want) {
				t.Errorf("MapSubtree(entries=%v) visited %v, want %v", tt.entries, got, tt.want)
			}
		})
	}
}

// TestNodeIsEmpty checks that either values or children make a node non-empty.
func TestNodeIsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		values   []int
		addChild bool
		want     bool
	}{
		{name: "empty", want: true},
		{name: "value only", values: []int{0}},
		{name: "empty child", addChild: true},
		{name: "value and child", values: []int{1}, addChild: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := NewNode[int]()
			for _, value := range tt.values {
				if err := node.AddValue(value); err != nil {
					t.Errorf("AddValue(%d) during setup for IsEmpty(values=%v, child=%t) error = %v, want nil", value, tt.values, tt.addChild, err)
					return
				}
			}
			if tt.addChild {
				node.EnsureChild('a')
			}
			if got := node.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty(values=%v, child=%t) = %t, want %t", tt.values, tt.addChild, got, tt.want)
			}
		})
	}
}
