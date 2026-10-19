package hashmap

import "errors"

var (
	// ErrUninitialized indicates that a hashmap has not been initialized.
	ErrUninitialized = errors.New("hashmap is not initialized")
	// ErrKeyExists indicates that Insert rejected a key already in the hashmap.
	ErrKeyExists = errors.New("key already exists in hashmap")
	// ErrKeyNotFound indicates that Remove could not find the requested key.
	ErrKeyNotFound = errors.New("key not found in hashmap")

	errDuplicateKeyDuringResize = errors.New("duplicate key during resize")
)
