package mqueue

const (
	maxFrame  = 10
	frameSize = 4096
)

// priority orders the frames of a queue: a higher priority is received first.
type priority uint

const (
	// priorityNormal is the priority of ordinary frames.
	priorityNormal priority = iota
	// priorityHigh makes a frame overtake the frames already waiting.
	priorityHigh
)
