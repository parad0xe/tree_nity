package trie

import (
	"errors"
	"fmt"
)

// Trie indexes comparable values by byte prefixes.
// Values must be unique within each prefix.
// Use NewTrie to initialize a trie before calling its methods.
type Trie[T comparable] struct {
	root *Node[T]
}

// NewTrie returns an initialized trie with no values.
func NewTrie[T comparable]() *Trie[T] {
	return &Trie[T]{
		root: NewNode[T](),
	}
}

// Insert associates value with prefix, rejecting duplicates at that prefix.
func (t *Trie[T]) Insert(prefix string, value T) error {
	node, err := t.MapPath(prefix, nil, func(parent *Node[T], key byte) *Node[T] {
		return parent.EnsureChild(key)
	})
	if err != nil {
		return err
	}

	return node.AddValue(value)
}

// Remove deletes value at prefix and prunes branches left without values or children.
func (t *Trie[T]) Remove(prefix string, value T) error {
	parents := make([]*Node[T], 0, len(prefix)+1)

	node, err := t.MapPath(prefix, func(n *Node[T]) {
		parents = append(parents, n)
	}, nil)
	if err != nil {
		return err
	}
	if node == nil {
		return fmt.Errorf("prefix %q not found in trie", prefix)
	}

	if !node.RemoveValue(value) {
		return errors.New("value not found in trie")
	}

	for i := len(parents) - 1; i > 0; i-- {
		currentNode := parents[i]
		if !currentNode.IsEmpty() {
			break
		}
		delete(parents[i-1].children, prefix[i-1])
	}

	return nil
}

// Find returns the node at prefix or an error if the path does not exist.
func (t *Trie[T]) Find(prefix string) (*Node[T], error) {
	node, err := t.MapPath(prefix, nil, nil)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, fmt.Errorf("prefix %q not found in trie", prefix)
	}
	return node, nil
}

// MapPath visits the root and each node along prefix when visit is non-nil.
// For an absent child, missing supplies a node given its parent and key byte.
// If missing is nil or returns nil, traversal stops with a nil node and no error.
func (t *Trie[T]) MapPath(prefix string, visit func(*Node[T]), missing func(*Node[T], byte) *Node[T]) (*Node[T], error) {
	if err := t.validate(); err != nil {
		return nil, err
	}

	currentNode := t.root
	if visit != nil {
		visit(currentNode)
	}

	for i := 0; i < len(prefix); i++ {
		child := currentNode.children[prefix[i]]
		if child == nil {
			if missing == nil {
				return nil, nil
			}
			child = missing(currentNode, prefix[i])
			if child == nil {
				return nil, nil
			}
		}
		currentNode = child
		if visit != nil {
			visit(currentNode)
		}
	}

	return currentNode, nil
}

// MapSubtree visits values stored at prefix and all its descendants in no fixed order.
func (t *Trie[T]) MapSubtree(prefix string, visit func(T)) error {
	node, err := t.Find(prefix)
	if err != nil {
		return err
	}

	node.MapSubtree(visit)
	return nil
}

func (t *Trie[T]) validate() error {
	if t == nil || t.root == nil {
		return errors.New("trie is not initialized")
	}
	return nil
}
