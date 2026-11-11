package topic

import (
	"errors"
)

var (
	// ErrUninitialized indicates that a topic has not been initialized.
	ErrUninitialized = errors.New("topic is not initialized")
	// ErrInvalidConsumer indicates that a consumer, its ID, or its connection is invalid.
	ErrInvalidConsumer = errors.New("consumer is invalid")
	// ErrConsumerTrieUninitialized indicates that a consumer trie has not been initialized.
	ErrConsumerTrieUninitialized = errors.New("consumer trie is not initialized")
	// ErrEmptyConsumerID indicates that a subscription has no consumer identifier.
	ErrEmptyConsumerID = errors.New("consumer ID is empty")
	// ErrMessageOffsetTrieUninitialized indicates an uninitialized message offset trie.
	ErrMessageOffsetTrieUninitialized = errors.New("message offset trie is not initialized")
)
