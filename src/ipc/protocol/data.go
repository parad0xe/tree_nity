package protocol

import (
	"encoding/json"
	"fmt"
)

// Data holds a frame's opcode, emitter, decoded metadata, and payload.
// Use Encode and Decode for JSON serialization.
type Data struct {
	opcode   Opcode
	emitter  Emitter
	metadata Metadata
	payload  Payload
}

// NewData creates a validated frame and infers its emitter from metadata.
// Metadata must be *ClientMetadata or *ServerMetadata, and payload must match opcode.
// Opcodes are interpreted by emitter; no server opcodes are defined yet.
// On error, it returns a zero Data and an error inspectable with errors.Is.
func NewData(opcode Opcode, metadata Metadata, payload Payload) (Data, error) {
	data := Data{opcode: opcode, metadata: metadata, payload: payload}
	switch metadata.(type) {
	case *ClientMetadata:
		data.emitter = Client
	case *ServerMetadata:
		data.emitter = Server
	case nil:
		return Data{}, ErrMissingMetadata
	default:
		return Data{}, fmt.Errorf("%w: %T", ErrInvalidMetadata, metadata)
	}
	if _, err := data.toJSON(); err != nil {
		return Data{}, err
	}
	return data, nil
}

// Opcode returns the frame's command code.
func (d Data) Opcode() Opcode {
	return d.opcode
}

// Emitter returns the frame's sender type.
func (d Data) Emitter() Emitter {
	return d.emitter
}

// Metadata returns the frame's emitter-specific metadata.
func (d Data) Metadata() Metadata {
	return d.metadata
}

// Payload returns the frame's command-specific payload.
func (d Data) Payload() Payload {
	return d.payload
}

func (d Data) toJSON() (dataJSON, error) {
	if err := d.validate(); err != nil {
		return dataJSON{}, err
	}

	tmpMetadata, err := d.metadata.toJSON(d.emitter)
	if err != nil {
		return dataJSON{}, err
	}
	if err := d.opcode.validate(d.emitter); err != nil {
		return dataJSON{}, err
	}
	tmpPayload, err := d.payload.toJSON(d.opcode)
	if err != nil {
		return dataJSON{}, err
	}

	metadata, err := json.Marshal(tmpMetadata)
	if err != nil {
		return dataJSON{}, err
	}
	payload, err := json.Marshal(tmpPayload)
	if err != nil {
		return dataJSON{}, err
	}
	rawMetadata := json.RawMessage(metadata)
	rawPayload := json.RawMessage(payload)

	return dataJSON{
		Opcode:   &d.opcode,
		Emitter:  &d.emitter,
		Metadata: &rawMetadata,
		Payload:  &rawPayload,
	}, nil
}

func (d Data) validate() error {
	switch d.emitter {
	case Client, Server:
	default:
		return fmt.Errorf("%w: %d", ErrUnknownEmitter, d.emitter)
	}
	if d.metadata == nil {
		return ErrMissingMetadata
	}
	if d.payload == nil {
		return ErrMissingPayload
	}
	return nil
}
