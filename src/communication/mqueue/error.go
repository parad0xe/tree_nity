package mqueue

import "errors"

var (
	// ErrInvalidName reports a queue name that does not follow the POSIX naming rules.
	ErrInvalidName = errors.New("invalid queue name")
	// ErrEmptyMessage reports an attempt to send a message with no content.
	ErrEmptyMessage = errors.New("message is empty")
	// ErrMessageTooLarge reports a message longer than the message size of the queue.
	ErrMessageTooLarge = errors.New("message too large")
)
