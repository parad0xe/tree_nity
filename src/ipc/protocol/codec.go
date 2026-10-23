package protocol

import (
	"bytes"
	"encoding/json"
	"io"
)

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
