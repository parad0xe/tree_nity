package consumer

import "errors"

var (
	// ErrUninitialized indicates that a consumer has not been initialized.
	ErrUninitialized = errors.New("consumer is not initialized")
	// ErrInfoUninitialized indicates that consumer metadata has not been initialized.
	ErrInfoUninitialized = errors.New("consumer info is not initialized")
	// ErrConnectionUninitialized indicates that a consumer connection has not been initialized.
	ErrConnectionUninitialized = errors.New("consumer connection is not initialized")
	// ErrManagerUninitialized indicates that a consumer manager has not been initialized.
	ErrManagerUninitialized = errors.New("consumer manager is not initialized")
	// ErrNotFound indicates that the requested consumer is absent from the manager.
	ErrNotFound = errors.New("consumer not found")
)
