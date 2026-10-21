package ipc

import "errors"

var (
	// ErrInvalidFrame reports a received frame that is not valid protocol data.
	ErrInvalidFrame = errors.New("invalid frame")
	// ErrInvalidData reports protocol data that cannot be encoded into a frame.
	ErrInvalidData = errors.New("invalid data")
)
