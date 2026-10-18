package topic

import (
	"errors"
	"slices"

	"tree_nity/src/server/utils/trie"
)

// MessageOffsetTrie indexes message offsets by key for prefix searches.
type MessageOffsetTrie struct {
	trie *trie.Trie[int32]
}

// NewMessageOffsetTrie returns an initialized, empty message offset index.
func NewMessageOffsetTrie() *MessageOffsetTrie {
	return &MessageOffsetTrie{
		trie: trie.NewTrie[int32](),
	}
}

// Insert associates offset with key, rejecting duplicates for the same key.
func (t *MessageOffsetTrie) Insert(key string, offset int32) error {
	if err := t.validate(); err != nil {
		return err
	}

	return t.trie.Insert(key, offset)
}

// Search returns sorted offsets >= offset for keys starting with prefix.
func (t *MessageOffsetTrie) Search(prefix string, offset int32) ([]int32, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}

	node, err := t.trie.MapPath(prefix, nil, nil)
	if err != nil {
		return nil, err
	}

	offsets := make([]int32, 0)
	if node == nil {
		return offsets, nil
	}

	node.MapSubtree(func(value int32) {
		if value >= offset {
			offsets = append(offsets, value)
		}
	})

	slices.Sort(offsets)
	return offsets, nil
}

func (t *MessageOffsetTrie) validate() error {
	if t == nil || t.trie == nil {
		return errors.New("message offset trie is not initialized")
	}
	return nil
}
