package id

import (
	"hash/fnv"
	"regexp"
)

var validPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`)

// ID is a client identifier shared by the client and server.
type ID string

// Hash returns a deterministic 64-bit FNV-1a hash of the identifier.
func (id ID) Hash() uint64 {
	h := fnv.New64a()
	// Writes to a hash.Hash never return an error.
	_, _ = h.Write([]byte(id))
	return h.Sum64()
}

// Validate checks that the identifier contains 1 to 32 ASCII letters, digits,
// underscores, dots, or hyphens.
// It returns ErrInvalidID when the identifier does not match this format.
func (id ID) Validate() error {
	if !validPattern.MatchString(string(id)) {
		return ErrInvalidID
	}
	return nil
}
