package hashmap

type slotState uint8

const (
	slotEmpty slotState = iota
	slotOccupied
	slotDeleted
)

type entry[K Hashable, V any] struct {
	key   K
	value V
	state slotState
}
