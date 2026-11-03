package protocol

// Payload holds operation-specific frame data.
// Each concrete payload type must match the frame's emitter and opcode.
type Payload interface {
	toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error)
	validate() error
}
