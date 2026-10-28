package protocol

import (
	"bytes"
	"encoding/json"
	"io"
)

// Encode validates data and encodes it as a single JSON frame.
// Metadata and payload must match the emitter and opcode and contain valid UTF-8.
// Opcodes are interpreted by emitter; no server opcodes are defined yet.
// Messages must respect MessageSize; offsets and transport size limits are excluded.
// Validation errors support errors.Is. On error, the frame is nil.
func Encode(data Data) ([]byte, error) {
	tmpData, err := data.toJSON()
	if err != nil {
		return nil, err
	}

	return json.Marshal(tmpData)
}

// Decode decodes and validates a single JSON frame.
// Unknown fields, missing required fields, and trailing JSON values are rejected.
// Metadata is selected by emitter, and payload is selected by emitter and opcode.
// Server opcodes are not defined yet and are rejected with ErrUnknownOpcode.
// Errors for missing fields, unknown opcodes or emitters, oversized messages,
// and additional JSON values can be matched with errors.Is.
func Decode(frame []byte) (Data, error) {
	var tmpData dataJSON

	decoder := json.NewDecoder(bytes.NewReader(frame))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&tmpData); err != nil {
		return Data{}, err
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			return Data{}, ErrAdditionalJSONValue
		}
		return Data{}, err
	}

	return tmpData.toData()
}
