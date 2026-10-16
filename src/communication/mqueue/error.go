package mqueue

import "errors"

var (
	// ErrInvalidName reports a queue name that does not follow the POSIX naming rules.
	ErrInvalidName = errors.New("invalid queue name")
)
