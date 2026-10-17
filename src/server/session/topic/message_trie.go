package topic

import (
	"errors"
	"slices"

	"tree_nity/src/server/utils/trie"
)

type MessageOffsetTrie struct {
	tree *trie.Trie[int32]
}

func NewMessageOffsetTrie() *MessageOffsetTrie {
	return &MessageOffsetTrie{
		tree: trie.NewTrie[int32](),
	}
}

func (t *MessageOffsetTrie) Insert(key string, offset int32) error {
	if err := t.validate(); err != nil {
		return err
	}

	return t.tree.Insert(key, offset)
}

func (t *MessageOffsetTrie) Search(prefix string, offset int32) ([]int32, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}

	node, err := t.tree.MapPath(prefix, nil, nil)
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
	if t == nil || t.tree == nil {
		return errors.New("message offset trie is not initialized")
	}
	return nil
}
