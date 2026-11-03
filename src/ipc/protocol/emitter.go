package protocol

// Emitter identifies the sender of a protocol frame.
type Emitter int

const (
	// Client identifies a client frame.
	Client Emitter = iota
	// Server identifies a server frame.
	Server
)
