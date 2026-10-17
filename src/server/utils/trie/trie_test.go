package trie

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func newTestTrie(t *testing.T, entries map[string][]int) *Trie[int] {
	t.Helper()
	trie := NewTrie[int]()
	for prefix, values := range entries {
		for _, value := range values {
			if err := trie.Insert(prefix, value); err != nil {
				t.Errorf("Insert(%q, %d) during setup error = %v, want nil", prefix, value, err)
				return nil
			}
		}
	}
	return trie
}

// TestNewTrie checks that construction creates an empty root.
func TestNewTrie(t *testing.T) {
	trie := NewTrie[int]()
	if trie == nil || trie.root == nil {
		t.Errorf("NewTrie[int]() = %#v, want a trie with an initialized root", trie)
		return
	}
	if !trie.root.IsEmpty() {
		t.Errorf("NewTrie[int]().root.IsEmpty() = false, want true (values=%v, children=%v)", trie.root.values, trie.root.children)
	}
}

// TestTrieUninitialized checks errors and callback suppression for unusable tries.
func TestTrieUninitialized(t *testing.T) {
	tests := []struct {
		name string
		trie *Trie[int]
	}{
		{name: "nil receiver"},
		{name: "zero value", trie: &Trie[int]{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callbackCalled := false
			operations := []struct {
				name  string
				input string
				run   func() (*Node[int], error)
			}{
				{name: "Insert", input: `"a", 1`, run: func() (*Node[int], error) {
					return nil, tt.trie.Insert("a", 1)
				}},
				{name: "Remove", input: `"a", 1`, run: func() (*Node[int], error) {
					return nil, tt.trie.Remove("a", 1)
				}},
				{name: "Find", input: `"a"`, run: func() (*Node[int], error) {
					return tt.trie.Find("a")
				}},
				{name: "MapPath", input: `"a", visit, missing`, run: func() (*Node[int], error) {
					return tt.trie.MapPath("a", func(*Node[int]) {
						callbackCalled = true
					}, func(*Node[int], byte) *Node[int] {
						callbackCalled = true
						return nil
					})
				}},
				{name: "MapSubtree", input: `"a", visit`, run: func() (*Node[int], error) {
					return nil, tt.trie.MapSubtree("a", func(int) {
						callbackCalled = true
					})
				}},
			}
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					callbackCalled = false
					got, err := operation.run()
					if err == nil || err.Error() != "trie is not initialized" {
						t.Errorf("%s(%s) on %s error = %v, want %q", operation.name, operation.input, tt.name, err, "trie is not initialized")
					}
					if got != nil {
						t.Errorf("%s(%s) on %s returned node %p, want nil", operation.name, operation.input, tt.name, got)
					}
					if callbackCalled {
						t.Errorf("%s(%s) on %s called a callback, want no callback calls", operation.name, operation.input, tt.name)
					}
				})
			}
		})
	}
}

// TestTrieInsertFind checks values at empty, byte-oriented, and long prefixes.
func TestTrieInsertFind(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		values    []int
		want      []int
		wantErrAt map[int]bool
	}{
		{name: "empty prefix", values: []int{0}, want: []int{0}},
		{name: "single byte", prefix: "a", values: []int{1}, want: []int{1}},
		{name: "multiple values", prefix: "topic", values: []int{1, 2, 3}, want: []int{1, 2, 3}},
		{name: "duplicate at root", values: []int{0, 0}, want: []int{0}, wantErrAt: map[int]bool{1: true}},
		{name: "duplicates", prefix: "topic", values: []int{1, 1, 2, 1}, want: []int{1, 2}, wantErrAt: map[int]bool{1: true, 3: true}},
		{name: "UTF-8", prefix: "é/猫", values: []int{1}, want: []int{1}},
		{name: "binary bytes", prefix: "\x00\xff", values: []int{1}, want: []int{1}},
		{name: "long prefix", prefix: strings.Repeat("a", 4096), values: []int{1}, want: []int{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := NewTrie[int]()
			for i, value := range tt.values {
				err := trie.Insert(tt.prefix, value)
				if tt.wantErrAt[i] {
					if err == nil || err.Error() != "value already exists" {
						t.Errorf("Insert(%q, %d) at index %d in %v error = %v, want %q", tt.prefix, value, i, tt.values, err, "value already exists")
					}
				} else if err != nil {
					t.Errorf("Insert(%q, %d) at index %d in %v error = %v, want nil", tt.prefix, value, i, tt.values, err)
				}
			}
			node, err := trie.Find(tt.prefix)
			if err != nil || node == nil {
				t.Errorf("Find(%q) after Insert values %v = %p, error = %v, want a node and nil error", tt.prefix, tt.values, node, err)
				return
			}
			if !slices.Equal(node.values, tt.want) {
				t.Errorf("Find(%q) after Insert values %v returned values %v, want %v", tt.prefix, tt.values, node.values, tt.want)
			}
			if tt.prefix == "" && node != trie.root {
				t.Errorf("Find(%q) = %p, want root %p", tt.prefix, node, trie.root)
			}
		})
	}
}

// TestTrieInsertAfterRemove checks that removing a value permits its insertion again.
func TestTrieInsertAfterRemove(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
	}{
		{name: "root"},
		{name: "leaf", prefix: "topic"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestTrie(t, map[string][]int{tt.prefix: {1}})
			if trie == nil {
				return
			}
			if err := trie.Insert(tt.prefix, 1); err == nil || err.Error() != "value already exists" {
				t.Errorf("Insert(%q, 1) with an existing value error = %v, want %q", tt.prefix, err, "value already exists")
			}
			if err := trie.Remove(tt.prefix, 1); err != nil {
				t.Errorf("Remove(%q, 1) after a rejected duplicate error = %v, want nil", tt.prefix, err)
				return
			}
			if err := trie.Insert(tt.prefix, 1); err != nil {
				t.Errorf("Insert(%q, 1) after Remove error = %v, want nil", tt.prefix, err)
			}
			node, err := trie.Find(tt.prefix)
			if err != nil || node == nil {
				t.Errorf("Find(%q) after reinsertion = %p, error = %v, want a node and nil error", tt.prefix, node, err)
				return
			}
			if !slices.Equal(node.values, []int{1}) {
				t.Errorf("Find(%q) after reinsertion values = %v, want [1]", tt.prefix, node.values)
			}
		})
	}
}

// TestTrieFind checks internal prefixes and absent paths without creating nodes.
func TestTrieFind(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		want    []int
		wantErr bool
	}{
		{name: "root"},
		{name: "stored prefix", prefix: "cart", want: []int{1}},
		{name: "shared prefix", prefix: "car"},
		{name: "internal prefix", prefix: "carp"},
		{name: "missing branch", prefix: "cat", wantErr: true},
		{name: "beyond stored prefix", prefix: "carton", wantErr: true},
		{name: "missing first byte", prefix: "x", wantErr: true},
		{name: "case sensitive", prefix: "Cart", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestTrie(t, map[string][]int{"cart": {1}, "carpet": {2}})
			if trie == nil {
				return
			}
			node, err := trie.Find(tt.prefix)
			if tt.wantErr {
				wantErr := fmt.Sprintf("prefix %q not found in trie", tt.prefix)
				if err == nil || err.Error() != wantErr || node != nil {
					t.Errorf("Find(%q) = %p, error = %v, want nil and %q", tt.prefix, node, err, wantErr)
				}
				return
			}
			if err != nil || node == nil {
				t.Errorf("Find(%q) = %p, error = %v, want a node and nil error", tt.prefix, node, err)
				return
			}
			if !slices.Equal(node.values, tt.want) {
				t.Errorf("Find(%q) values = %v, want %v", tt.prefix, node.values, tt.want)
			}
		})
	}
}

// TestTrieRemove checks prefix-scoped values, shared branches, pruning, and failures.
func TestTrieRemove(t *testing.T) {
	tests := []struct {
		name       string
		entries    map[string][]int
		prefix     string
		value      int
		wantNodes  map[string][]int
		wantAbsent []string
		wantErr    string
	}{
		{
			name: "last leaf", entries: map[string][]int{"abc": {1}}, prefix: "abc", value: 1,
			wantNodes: map[string][]int{"": {}}, wantAbsent: []string{"a", "ab", "abc"},
		},
		{
			name: "shared branch", entries: map[string][]int{"ab": {1}, "ac": {2}}, prefix: "ab", value: 1,
			wantNodes: map[string][]int{"": {}, "a": {}, "ac": {2}}, wantAbsent: []string{"ab"},
		},
		{
			name: "ancestor retains value", entries: map[string][]int{"a": {1}, "ab": {2}}, prefix: "ab", value: 2,
			wantNodes: map[string][]int{"": {}, "a": {1}}, wantAbsent: []string{"ab"},
		},
		{
			name: "parent retains children", entries: map[string][]int{"a": {1}, "ab": {2}}, prefix: "a", value: 1,
			wantNodes: map[string][]int{"": {}, "a": {}, "ab": {2}},
		},
		{
			name: "other value remains", entries: map[string][]int{"a": {1, 2}}, prefix: "a", value: 1,
			wantNodes: map[string][]int{"": {}, "a": {2}},
		},
		{
			name: "same value at another prefix", entries: map[string][]int{"a": {1}, "b": {1}}, prefix: "a", value: 1,
			wantNodes: map[string][]int{"": {}, "b": {1}}, wantAbsent: []string{"a"},
		},
		{
			name: "root only", entries: map[string][]int{"": {0}}, value: 0,
			wantNodes: map[string][]int{"": {}},
		},
		{
			name: "root retains children", entries: map[string][]int{"": {1}, "a": {2}}, value: 1,
			wantNodes: map[string][]int{"": {}, "a": {2}},
		},
		{
			name: "missing prefix", entries: map[string][]int{"a": {1}}, prefix: "b", value: 1,
			wantNodes: map[string][]int{"": {}, "a": {1}}, wantAbsent: []string{"b"}, wantErr: `prefix "b" not found in trie`,
		},
		{
			name: "missing value", entries: map[string][]int{"a": {1}}, prefix: "a", value: 2,
			wantNodes: map[string][]int{"": {}, "a": {1}}, wantErr: "value not found in trie",
		},
		{
			name: "empty internal prefix", entries: map[string][]int{"ab": {1}}, prefix: "a", value: 1,
			wantNodes: map[string][]int{"": {}, "a": {}, "ab": {1}}, wantErr: "value not found in trie",
		},
		{
			name: "empty trie", value: 0,
			wantNodes: map[string][]int{"": {}}, wantErr: "value not found in trie",
		},
		{
			name: "UTF-8 shared branch", entries: map[string][]int{"é/猫": {1}, "é/犬": {2}}, prefix: "é/猫", value: 1,
			wantNodes: map[string][]int{"": {}, "é": {}, "é/": {}, "é/犬": {2}}, wantAbsent: []string{"é/猫"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestTrie(t, tt.entries)
			if trie == nil {
				return
			}
			root := trie.root
			err := trie.Remove(tt.prefix, tt.value)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Errorf("Remove(%q, %d) with entries %v error = %v, want %q", tt.prefix, tt.value, tt.entries, err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("Remove(%q, %d) with entries %v error = %v, want nil", tt.prefix, tt.value, tt.entries, err)
			}
			if trie.root == nil || trie.root != root {
				t.Errorf("Remove(%q, %d) root = %p, want original root %p", tt.prefix, tt.value, trie.root, root)
				return
			}

			var wantValues []int
			for prefix, want := range tt.wantNodes {
				wantValues = append(wantValues, want...)
				node, err := trie.Find(prefix)
				if err != nil || node == nil {
					t.Errorf("Find(%q) after Remove(%q, %d) = %p, error = %v, want a node and nil error", prefix, tt.prefix, tt.value, node, err)
					continue
				}
				if !slices.Equal(node.values, want) {
					t.Errorf("Find(%q) after Remove(%q, %d) values = %v, want %v", prefix, tt.prefix, tt.value, node.values, want)
				}
			}
			for _, prefix := range tt.wantAbsent {
				if node, err := trie.Find(prefix); err == nil || node != nil {
					t.Errorf("Find(%q) after Remove(%q, %d) = %p, error = %v, want nil and an error", prefix, tt.prefix, tt.value, node, err)
				}
			}

			var gotValues []int
			if err := trie.MapSubtree("", func(value int) {
				gotValues = append(gotValues, value)
			}); err != nil {
				t.Errorf("MapSubtree(%q) after Remove(%q, %d) error = %v, want nil", "", tt.prefix, tt.value, err)
			}
			slices.Sort(gotValues)
			slices.Sort(wantValues)
			if !slices.Equal(gotValues, wantValues) {
				t.Errorf("Remove(%q, %d) with entries %v left subtree values %v, want %v", tt.prefix, tt.value, tt.entries, gotValues, wantValues)
			}
			wantEmpty := len(tt.wantNodes) == 1 && len(wantValues) == 0
			if got := root.IsEmpty(); got != wantEmpty {
				t.Errorf("Remove(%q, %d) with entries %v left root IsEmpty() = %t, want %t", tt.prefix, tt.value, tt.entries, got, wantEmpty)
			}
		})
	}
}

// TestTrieMapPath checks ordered visits and missing paths without a creation callback.
func TestTrieMapPath(t *testing.T) {
	trie := newTestTrie(t, map[string][]int{"ab": {1}})
	if trie == nil {
		return
	}
	a := trie.root.children['a']
	b := a.children['b']
	tests := []struct {
		name    string
		prefix  string
		want    *Node[int]
		visited []*Node[int]
	}{
		{name: "root", want: trie.root, visited: []*Node[int]{trie.root}},
		{name: "internal node", prefix: "a", want: a, visited: []*Node[int]{trie.root, a}},
		{name: "leaf", prefix: "ab", want: b, visited: []*Node[int]{trie.root, a, b}},
		{name: "missing suffix", prefix: "ac", visited: []*Node[int]{trie.root, a}},
		{name: "missing first byte", prefix: "x", visited: []*Node[int]{trie.root}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var visited []*Node[int]
			got, err := trie.MapPath(tt.prefix, func(node *Node[int]) {
				visited = append(visited, node)
			}, nil)
			if err != nil || got != tt.want {
				t.Errorf("MapPath(%q, visit, nil) = %p, error = %v, want %p and nil error", tt.prefix, got, err, tt.want)
			}
			if !slices.Equal(visited, tt.visited) {
				t.Errorf("MapPath(%q, visit, nil) visited %v, want %v", tt.prefix, visited, tt.visited)
			}
			if len(trie.root.children) != 1 || len(a.children) != 1 {
				t.Errorf("MapPath(%q, visit, nil) child counts at root and a = %d and %d, want 1 and 1", tt.prefix, len(trie.root.children), len(a.children))
			}
		})
	}
}

// TestTrieMapPathCreatesMissing checks callbacks for new paths and existing bytes.
func TestTrieMapPathCreatesMissing(t *testing.T) {
	tests := []struct {
		name        string
		existing    string
		prefix      string
		wantMissing string
	}{
		{name: "root"},
		{name: "new path", prefix: "ab", wantMissing: "ab"},
		{name: "existing path", existing: "ab", prefix: "ab"},
		{name: "new suffix", existing: "ab", prefix: "abc", wantMissing: "c"},
		{name: "UTF-8 bytes", existing: "é", prefix: "é/猫", wantMissing: "/猫"},
		{name: "binary bytes", prefix: "\x00\xff", wantMissing: "\x00\xff"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestTrie(t, map[string][]int{tt.existing: {7}})
			if trie == nil {
				return
			}
			var visited []*Node[int]
			var missing []byte
			got, err := trie.MapPath(tt.prefix, func(node *Node[int]) {
				visited = append(visited, node)
			}, func(parent *Node[int], key byte) *Node[int] {
				if len(visited) == 0 || parent != visited[len(visited)-1] {
					t.Errorf("MapPath(%q) missing callback for byte %d received parent %p, want the last visited node in %v", tt.prefix, key, parent, visited)
				}
				missing = append(missing, key)
				return parent.EnsureChild(key)
			})
			if err != nil || got == nil {
				t.Errorf("MapPath(%q, visit, EnsureChild) = %p, error = %v, want a node and nil error", tt.prefix, got, err)
			}
			if !slices.Equal(missing, []byte(tt.wantMissing)) {
				t.Errorf("MapPath(%q) with existing prefix %q requested missing bytes %v, want %v", tt.prefix, tt.existing, missing, []byte(tt.wantMissing))
			}
			if len(visited) != len(tt.prefix)+1 {
				t.Errorf("MapPath(%q) visit count = %d, want %d", tt.prefix, len(visited), len(tt.prefix)+1)
			} else {
				if visited[0] != trie.root || visited[len(visited)-1] != got {
					t.Errorf("MapPath(%q) visited endpoints %p and %p, want root %p and result %p", tt.prefix, visited[0], visited[len(visited)-1], trie.root, got)
				}
				for i := 0; i < len(tt.prefix); i++ {
					if visited[i].children[tt.prefix[i]] != visited[i+1] {
						t.Errorf("MapPath(%q) visited node %p at byte index %d, want child %p", tt.prefix, visited[i+1], i, visited[i].children[tt.prefix[i]])
					}
				}
			}
			if node, err := trie.Find(tt.prefix); err != nil || node != got {
				t.Errorf("Find(%q) after MapPath = %p, error = %v, want %p and nil error", tt.prefix, node, err, got)
			}
		})
	}
}

// TestTrieMapPathDeclinedMissing checks early termination when creation is declined.
func TestTrieMapPathDeclinedMissing(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		prefix   string
	}{
		{name: "at root", prefix: "ab"},
		{name: "after existing prefix", existing: "a", prefix: "abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestTrie(t, map[string][]int{tt.existing: {7}})
			if trie == nil {
				return
			}
			parent, err := trie.Find(tt.existing)
			if err != nil || parent == nil {
				t.Errorf("Find(%q) during setup = %p, error = %v, want a node and nil error", tt.existing, parent, err)
				return
			}
			missingCalls := 0
			var visited []*Node[int]
			got, err := trie.MapPath(tt.prefix, func(node *Node[int]) {
				visited = append(visited, node)
			}, func(node *Node[int], key byte) *Node[int] {
				missingCalls++
				if node != parent || key != tt.prefix[len(tt.existing)] {
					t.Errorf("MapPath(%q) missing callback = (%p, %d), want (%p, %d)", tt.prefix, node, key, parent, tt.prefix[len(tt.existing)])
				}
				return nil
			})
			if err != nil || got != nil {
				t.Errorf("MapPath(%q) with a declining callback = %p, error = %v, want nil and nil error", tt.prefix, got, err)
			}
			if missingCalls != 1 || len(visited) != len(tt.existing)+1 {
				t.Errorf("MapPath(%q) with existing prefix %q made %d missing calls and %d visits, want 1 and %d", tt.prefix, tt.existing, missingCalls, len(visited), len(tt.existing)+1)
			}
			if len(parent.children) != 0 || !slices.Equal(parent.values, []int{7}) {
				t.Errorf("MapPath(%q) with a declining callback left parent values %v and children %v, want [7] and no children", tt.prefix, parent.values, parent.children)
			}
		})
	}
}

// TestTrieMapSubtree checks prefix selection and visits without relying on map order.
func TestTrieMapSubtree(t *testing.T) {
	entries := map[string][]int{"": {9}, "a": {1}, "ab": {1, 2}, "ac": {3}, "b": {4}}
	tests := []struct {
		name    string
		entries map[string][]int
		prefix  string
		want    []int
		wantErr bool
	}{
		{name: "empty trie"},
		{name: "single value", entries: map[string][]int{"a": {0}}, prefix: "a", want: []int{0}},
		{name: "entire trie", entries: entries, want: []int{1, 1, 2, 3, 4, 9}},
		{name: "equal values at different prefixes", entries: entries, prefix: "a", want: []int{1, 1, 2, 3}},
		{name: "leaf", entries: entries, prefix: "ab", want: []int{1, 2}},
		{name: "sibling", entries: entries, prefix: "b", want: []int{4}},
		{name: "internal node without values", entries: map[string][]int{"ab": {2}}, prefix: "a", want: []int{2}},
		{name: "missing prefix", entries: entries, prefix: "z", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestTrie(t, tt.entries)
			if trie == nil {
				return
			}
			var got []int
			err := trie.MapSubtree(tt.prefix, func(value int) {
				got = append(got, value)
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("MapSubtree(%q) with entries %v error = %v, want error = %t", tt.prefix, tt.entries, err, tt.wantErr)
			}
			slices.Sort(got)
			if !slices.Equal(got, tt.want) {
				t.Errorf("MapSubtree(%q) with entries %v visited %v, want %v", tt.prefix, tt.entries, got, tt.want)
			}
		})
	}
}

// TestTrieManyEntries checks insertion, lookup, traversal, and complete cleanup at scale.
func TestTrieManyEntries(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "empty"},
		{name: "minimal", count: 1},
		{name: "large", count: 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := NewTrie[int]()
			want := make([]int, tt.count)
			for i := 0; i < tt.count; i++ {
				prefix := fmt.Sprintf("key/%04d", i)
				if err := trie.Insert(prefix, i); err != nil {
					t.Errorf("Insert(%q, %d) with %d entries error = %v, want nil", prefix, i, tt.count, err)
					return
				}
				want[i] = i
			}
			var got []int
			if err := trie.MapSubtree("", func(value int) {
				got = append(got, value)
			}); err != nil {
				t.Errorf("MapSubtree(%q) with %d entries error = %v, want nil", "", tt.count, err)
			}
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Errorf("MapSubtree(%q) with %d entries visited %v, want %v", "", tt.count, got, want)
			}
			for i := 0; i < tt.count; i++ {
				prefix := fmt.Sprintf("key/%04d", i)
				node, err := trie.Find(prefix)
				if err != nil || node == nil {
					t.Errorf("Find(%q) with %d entries = %p, error = %v, want a node and nil error", prefix, tt.count, node, err)
					return
				}
				if !slices.Equal(node.values, []int{i}) {
					t.Errorf("Find(%q) with %d entries values = %v, want [%d]", prefix, tt.count, node.values, i)
				}
				if err := trie.Remove(prefix, i); err != nil {
					t.Errorf("Remove(%q, %d) with %d entries error = %v, want nil", prefix, i, tt.count, err)
					return
				}
				if node, err := trie.Find(prefix); err == nil || node != nil {
					t.Errorf("Find(%q) after Remove(%q, %d) = %p, error = %v, want nil and an error", prefix, prefix, i, node, err)
				}
			}
			if trie.root == nil || !trie.root.IsEmpty() {
				t.Errorf("Remove all %d entries left root %#v, want an initialized empty root", tt.count, trie.root)
			}
		})
	}
}
