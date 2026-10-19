package mqueue

import "errors"

var (
	// ErrInvalidName reports a queue name that does not follow the POSIX naming rules.
	ErrInvalidName = errors.New("invalid queue name")
	// ErrEmptyFrame reports an attempt to send a frame with no content.
	ErrEmptyFrame = errors.New("frame is empty")
	// ErrFrameTooLarge reports a frame longer than the frame size of the queue.
	ErrFrameTooLarge = errors.New("frame too large")
)
