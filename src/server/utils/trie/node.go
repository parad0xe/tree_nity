package trie

type Node[T comparable] struct {
	children map[byte]*Node[T]
	values   []T
}

func NewNode[T comparable]() *Node[T] {
	return &Node[T]{
		children: make(map[byte]*Node[T]),
		values:   make([]T, 0),
	}
}

func (n *Node[T]) EnsureChild(key byte) *Node[T] {
	child := n.children[key]
	if child == nil {
		child = NewNode[T]()
		n.children[key] = child
	}
	return child
}

func (n *Node[T]) AddValue(value T) {
	n.values = append(n.values, value)
}

func (n *Node[T]) RemoveValue(value T) bool {
	for i, v := range n.values {
		if v == value {
			n.values = append(n.values[:i], n.values[i+1:]...)
			return true
		}
	}
	return false
}

func (n *Node[T]) MapSubtree(visit func(T)) {
	for _, v := range n.values {
		visit(v)
	}
	for _, child := range n.children {
		child.MapSubtree(visit)
	}
}

func (n *Node[T]) IsEmpty() bool {
	return len(n.values) == 0 && len(n.children) == 0
}
