package id

import "errors"

// ErrInvalidID indicates that a client identifier does not match the required format.
var ErrInvalidID = errors.New("invalid consumer ID")
