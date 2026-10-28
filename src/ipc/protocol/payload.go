package protocol

// Payload holds operation-specific frame data.
// Each concrete payload type must match the frame's opcode.
// The frame's emitter selects the opcode list used to validate the payload.
type Payload interface {
	toJSON(opcode Opcode) (payloadJSON, error)
	validate() error
}
