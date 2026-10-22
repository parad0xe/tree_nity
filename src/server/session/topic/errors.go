package topic

import "errors"

var (
	// ErrUninitialized indicates that a topic has not been initialized.
	ErrUninitialized = errors.New("topic is not initialized")
	// ErrInvalidConsumer indicates that a consumer, its ID, or its connection is invalid.
	ErrInvalidConsumer = errors.New("consumer is invalid")
)
