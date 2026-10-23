package protocol

import (
	"bytes"
	"encoding/json"
	"io"
)

// Encode validates data and encodes it as a single JSON frame.
// Metadata must match its emitter, and payload must match its opcode.
// Metadata and payload types implement their interfaces through pointer receivers.
// Strings must contain valid UTF-8, and messages must respect MessageSize.
// Message offsets are internal and are not encoded. Transport size limits are not checked.
// Nil metadata or payloads return ErrMissingMetadata or ErrMissingPayload.
// Incompatible types or invalid UTF-8 return ErrInvalidMetadata or ErrInvalidPayload.
// Unknown codes return ErrUnknownEmitter or ErrUnknownOpcode; oversized messages return
// ErrMessageTooLarge. These errors can be matched with errors.Is. On error, the frame is nil.
func Encode(data Data) ([]byte, error) {
	tmpData, err := data.toJSON()
	if err != nil {
		return nil, err
	}

	return json.Marshal(tmpData)
}

// Decode decodes and validates a single JSON frame.
// Unknown fields, missing required fields, and trailing JSON values are rejected.
// Metadata is selected by emitter, and payload is selected by opcode.
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
