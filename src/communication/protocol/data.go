package protocol

import (
	"encoding/json"
	"fmt"
)

// Data holds a frame's opcode, emitter, decoded metadata, and payload.
type Data struct {
	opcode   op
	emitter  emitter
	metadata Metadata
	payload  Payload
}

type dataJSON struct {
	// Opcode is nil when the operation code is missing or null.
	Opcode *op `json:"opcode"`
	// Emitter is nil when the sender type is missing or null.
	Emitter *emitter `json:"emitter"`
	// Metadata preserves emitter-specific JSON until its decoder is selected.
	Metadata *json.RawMessage `json:"metadata"`
	// Payload preserves operation-specific JSON until its decoder is selected.
	Payload *json.RawMessage `json:"payload"`
}

func (d dataJSON) validate() error {
	if d.Opcode == nil {
		return fmt.Errorf("missing opcode")
	}
	if d.Emitter == nil {
		return fmt.Errorf("missing emitter")
	}
	if d.Metadata == nil {
		return fmt.Errorf("missing metadata")
	}
	if d.Payload == nil {
		return fmt.Errorf("missing payload")
	}
	return nil
}

func (d dataJSON) toData() (Data, error) {
	if err := d.validate(); err != nil {
		return Data{}, err
	}

	tmpMetadata, err := newMetadataJSON(*d.Emitter, *d.Metadata)
	if err != nil {
		return Data{}, err
	}
	metadata, err := tmpMetadata.toMetadata()
	if err != nil {
		return Data{}, err
	}

	tmpPayload, err := newPayloadJSON(*d.Opcode, *d.Payload)
	if err != nil {
		return Data{}, err
	}
	payload, err := tmpPayload.toPayload()
	if err != nil {
		return Data{}, err
	}

	return Data{
		opcode:   *d.Opcode,
		emitter:  *d.Emitter,
		metadata: metadata,
		payload:  payload,
	}, nil
}
