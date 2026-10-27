package protocol

// Payload holds operation-specific data and converts it to JSON for its opcode.
// The frame's emitter selects the opcode list used to validate the payload.
type Payload interface {
	toJSON(opcode Opcode) (payloadJSON, error)
	validate() error
}

type payloadJSON interface {
	validate() error
	toPayload() (Payload, error)
}
