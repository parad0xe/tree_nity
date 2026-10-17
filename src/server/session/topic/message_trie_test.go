package topic

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"tree_nity/src/server/utils/trie"
)

const (
	lowestMessageOffset  int32 = -1 << 31
	highestMessageOffset int32 = 1<<31 - 1
)

func newTestMessageOffsetTrie(t *testing.T, entries map[string][]int32) *MessageOffsetTrie {
	t.Helper()
	trie := NewMessageOffsetTrie()
	for key, offsets := range entries {
		for _, offset := range offsets {
			if err := trie.Insert(key, offset); err != nil {
				t.Errorf("Insert(%q, %d) during setup error = %v, want nil", key, offset, err)
				return nil
			}
		}
	}
	return trie
}

// TestNewMessageOffsetTrie checks construction and an empty search result.
func TestNewMessageOffsetTrie(t *testing.T) {
	trie := NewMessageOffsetTrie()
	if trie == nil || trie.trie == nil {
		t.Errorf("NewMessageOffsetTrie() = %#v, want an initialized message offset trie", trie)
		return
	}
	got, err := trie.Search("", 0)
	if err != nil || len(got) != 0 {
		t.Errorf("Search(%q, %d) on a new message offset trie = %v, error = %v, want no offsets and nil error", "", 0, got, err)
	}
}

// TestMessageOffsetTrieUninitialized checks nil receivers and underlying trie errors.
func TestMessageOffsetTrieUninitialized(t *testing.T) {
	tests := []struct {
		name    string
		trie    *MessageOffsetTrie
		wantErr string
	}{
		{name: "nil receiver", wantErr: "message offset trie is not initialized"},
		{name: "zero value", trie: &MessageOffsetTrie{}, wantErr: "message offset trie is not initialized"},
		{name: "uninitialized inner trie", trie: &MessageOffsetTrie{trie: &trie.Trie[int32]{}}, wantErr: "trie is not initialized"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			operations := []struct {
				name string
				run  func() ([]int32, error)
			}{
				{name: "Insert", run: func() ([]int32, error) {
					return nil, tt.trie.Insert("a", 1)
				}},
				{name: "Search", run: func() ([]int32, error) {
					return tt.trie.Search("a", 1)
				}},
			}
			for _, operation := range operations {
				t.Run(operation.name, func(t *testing.T) {
					got, err := operation.run()
					if err == nil || err.Error() != tt.wantErr {
						t.Errorf("%s(%q, %d) on %s error = %v, want %q", operation.name, "a", 1, tt.name, err, tt.wantErr)
					}
					if got != nil {
						t.Errorf("%s(%q, %d) on %s returned %v, want nil", operation.name, "a", 1, tt.name, got)
					}
				})
			}
		})
	}
}

// TestMessageOffsetTrieInsert checks keys, integer limits, and duplicate rejection.
func TestMessageOffsetTrieInsert(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		offsets   []int32
		want      []int32
		wantErrAt map[int]bool
	}{
		{name: "empty key", offsets: []int32{0}, want: []int32{0}},
		{name: "single byte", key: "a", offsets: []int32{1}, want: []int32{1}},
		{name: "sort offsets", key: "topic", offsets: []int32{3, 1, 2}, want: []int32{1, 2, 3}},
		{name: "duplicate zero", offsets: []int32{0, 0}, want: []int32{0}, wantErrAt: map[int]bool{1: true}},
		{name: "duplicates", key: "topic", offsets: []int32{2, 2, 1, 2}, want: []int32{1, 2}, wantErrAt: map[int]bool{1: true, 3: true}},
		{name: "int32 limits", key: "bounds", offsets: []int32{highestMessageOffset, lowestMessageOffset, 0}, want: []int32{lowestMessageOffset, 0, highestMessageOffset}},
		{name: "UTF-8", key: "é/猫", offsets: []int32{1}, want: []int32{1}},
		{name: "binary bytes", key: "\x00\xff", offsets: []int32{1}, want: []int32{1}},
		{name: "long key", key: strings.Repeat("a", 4096), offsets: []int32{1}, want: []int32{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := NewMessageOffsetTrie()
			for i, offset := range tt.offsets {
				err := trie.Insert(tt.key, offset)
				if tt.wantErrAt[i] {
					if err == nil || err.Error() != "value already exists" {
						t.Errorf("Insert(%q, %d) at index %d in %v error = %v, want %q", tt.key, offset, i, tt.offsets, err, "value already exists")
					}
				} else if err != nil {
					t.Errorf("Insert(%q, %d) at index %d in %v error = %v, want nil", tt.key, offset, i, tt.offsets, err)
				}
			}
			got, err := trie.Search(tt.key, lowestMessageOffset)
			if err != nil || !slices.Equal(got, tt.want) {
				t.Errorf("Search(%q, %d) after inserting %v = %v, error = %v, want %v and nil error", tt.key, lowestMessageOffset, tt.offsets, got, err, tt.want)
			}
		})
	}
}

// TestMessageOffsetTrieSearch checks descendant prefixes, inclusive thresholds, and sorting.
func TestMessageOffsetTrieSearch(t *testing.T) {
	entries := map[string][]int32{"": {7}, "a": {9, 1}, "ab": {4}, "ac": {2}, "b": {3}}
	tests := []struct {
		name    string
		entries map[string][]int32
		prefix  string
		offset  int32
		want    []int32
	}{
		{name: "empty trie"},
		{name: "minimal", entries: map[string][]int32{"a": {0}}, prefix: "a", want: []int32{0}},
		{name: "entire trie", entries: entries, want: []int32{1, 2, 3, 4, 7, 9}},
		{name: "prefix and descendants", entries: entries, prefix: "a", want: []int32{1, 2, 4, 9}},
		{name: "ancestor excluded", entries: entries, prefix: "ab", want: []int32{4}},
		{name: "sibling", entries: entries, prefix: "b", want: []int32{3}},
		{name: "internal prefix", entries: map[string][]int32{"ab": {3}, "ac": {1}}, prefix: "a", want: []int32{1, 3}},
		{name: "missing prefix", entries: entries, prefix: "z"},
		{name: "beyond a stored key", entries: entries, prefix: "abc"},
		{name: "inclusive threshold", entries: entries, prefix: "a", offset: 4, want: []int32{4, 9}},
		{name: "threshold above all offsets", entries: entries, offset: 10},
		{name: "zero threshold", entries: map[string][]int32{"a": {-1, 0, 1}}, prefix: "a", want: []int32{0, 1}},
		{
			name: "minimum threshold", entries: map[string][]int32{"a": {highestMessageOffset, 0, lowestMessageOffset}},
			prefix: "a", offset: lowestMessageOffset, want: []int32{lowestMessageOffset, 0, highestMessageOffset},
		},
		{
			name: "maximum threshold", entries: map[string][]int32{"a": {lowestMessageOffset, highestMessageOffset, 0}},
			prefix: "a", offset: highestMessageOffset, want: []int32{highestMessageOffset},
		},
		{name: "UTF-8", entries: map[string][]int32{"é": {2}, "é/猫": {5}, "è": {1}}, prefix: "é", want: []int32{2, 5}},
		{name: "binary bytes", entries: map[string][]int32{"\x00": {2}, "\x00\xff": {1}, "\xff": {3}}, prefix: "\x00", want: []int32{1, 2}},
		{name: "case sensitive", entries: map[string][]int32{"Ab": {1}, "ab": {2}}, prefix: "a", want: []int32{2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trie := newTestMessageOffsetTrie(t, tt.entries)
			if trie == nil {
				return
			}
			got, err := trie.Search(tt.prefix, tt.offset)
			if err != nil {
				t.Errorf("Search(%q, %d) with entries %v error = %v, want nil", tt.prefix, tt.offset, tt.entries, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("Search(%q, %d) with entries %v = %v, want %v", tt.prefix, tt.offset, tt.entries, got, tt.want)
			}
		})
	}
}

// TestMessageOffsetTrieManyMessages checks sorted results and filtering at increasing sizes.
func TestMessageOffsetTrieManyMessages(t *testing.T) {
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
			trie := NewMessageOffsetTrie()
			want := make([]int32, tt.count)
			for i := 0; i < tt.count; i++ {
				key := fmt.Sprintf("topic/%04d", i)
				offset := int32(tt.count - 1 - i)
				if err := trie.Insert(key, offset); err != nil {
					t.Errorf("Insert(%q, %d) at index %d of %d error = %v, want nil", key, offset, i, tt.count, err)
					return
				}
				want[i] = int32(i)
			}
			got, err := trie.Search("", lowestMessageOffset)
			if err != nil || !slices.Equal(got, want) {
				t.Errorf("Search(%q, %d) with %d messages = %v, error = %v, want %v and nil error", "", lowestMessageOffset, tt.count, got, err, want)
			}
			threshold := int32(tt.count / 2)
			got, err = trie.Search("topic/", threshold)
			if err != nil || !slices.Equal(got, want[tt.count/2:]) {
				t.Errorf("Search(%q, %d) with %d messages = %v, error = %v, want %v and nil error", "topic/", threshold, tt.count, got, err, want[tt.count/2:])
			}
		})
	}
}
