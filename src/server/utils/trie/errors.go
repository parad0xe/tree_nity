package trie

import "errors"

var (
	// ErrUninitialized indicates that a trie has not been initialized.
	ErrUninitialized = errors.New("trie is not initialized")
	// ErrValueExists indicates that a node already contains the requested value.
	ErrValueExists = errors.New("value already exists")
	// ErrValueNotFound indicates that a value is absent at the requested prefix.
	ErrValueNotFound = errors.New("value not found in trie")
	// ErrPrefixNotFound indicates that a requested prefix does not exist.
	// Returned errors include the missing prefix and wrap ErrPrefixNotFound.
	ErrPrefixNotFound = errors.New("not found in trie")
)
