package id

import (
	"errors"
	"hash/fnv"
	"regexp"
)

type ID string

func (id ID) Hash() uint64 {
	h := fnv.New64a()
	// Writes to a hash.Hash never return an error.
	_, _ = h.Write([]byte(id))
	return h.Sum64()
}

func (id ID) Validate() error {
	if !regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,32}$`).MatchString(string(id)) {
		return errors.New("invalid consumer ID")
	}
	return nil
}
