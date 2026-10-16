package mqueue

const (
	maxMsg  = 10
	msgSize = 4096

	noOptions       option = 0
	createExclusive        = Create | Exclusive
)
