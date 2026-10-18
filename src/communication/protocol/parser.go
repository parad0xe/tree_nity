package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// NewDataFromFrame decodes and validates a single JSON frame.
// Unknown fields, missing required fields, and trailing JSON values are rejected.
// Metadata is selected by emitter, and payload is selected by opcode.
func NewDataFromFrame(frame []byte) (Data, error) {
	var tmpData dataJSON

	decoder := json.NewDecoder(bytes.NewReader(frame))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&tmpData); err != nil {
		return Data{}, err
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		if err == nil {
			return Data{}, fmt.Errorf("unexpected additional JSON value")
		}
		return Data{}, err
	}

	return tmpData.toData()
}
