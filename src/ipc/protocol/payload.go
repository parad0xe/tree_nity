package protocol

// Payload holds data for a specific emitter and opcode.
type Payload interface {
	toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error)
	validate() error
}
