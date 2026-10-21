package trie

import "slices"

// Node stores unique comparable values and byte-keyed child nodes.
type Node[T comparable] struct {
	children map[byte]*Node[T]
	values   []T
}

// NewNode returns an empty node ready to store values and children.
func NewNode[T comparable]() *Node[T] {
	return &Node[T]{
		children: make(map[byte]*Node[T]),
		values:   make([]T, 0),
	}
}

// EnsureChild returns the child for key, creating it if absent.
func (n *Node[T]) EnsureChild(key byte) *Node[T] {
	child := n.children[key]
	if child == nil {
		child = NewNode[T]()
		n.children[key] = child
	}
	return child
}

// AddValue appends value, returning an error if the node already contains it.
// Duplicate values return ErrValueExists.
func (n *Node[T]) AddValue(value T) error {
	if slices.Contains(n.values, value) {
		return ErrValueExists
	}
	n.values = append(n.values, value)
	return nil
}

// RemoveValue removes value and reports whether it was present.
func (n *Node[T]) RemoveValue(value T) bool {
	for i, v := range n.values {
		if v == value {
			n.values = append(n.values[:i], n.values[i+1:]...)
			return true
		}
	}
	return false
}

// Values returns a shallow copy of the node's values in insertion order.
func (n *Node[T]) Values() []T {
	return slices.Clone(n.values)
}

// MapSubtree visits every value in this node and its descendants.
// Values stored in multiple nodes are visited once per node.
// Child traversal order is unspecified.
func (n *Node[T]) MapSubtree(visit func(T)) {
	for _, v := range n.values {
		visit(v)
	}
	for _, child := range n.children {
		child.MapSubtree(visit)
	}
}

// IsEmpty reports whether the node has neither values nor children.
func (n *Node[T]) IsEmpty() bool {
	return len(n.values) == 0 && len(n.children) == 0
}
