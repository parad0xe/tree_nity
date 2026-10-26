package protocol

import "encoding/json"

// Data holds a frame's opcode, emitter, decoded metadata, and payload.
type Data struct {
	opcode   Opcode
	emitter  emitter
	metadata Metadata
	payload  Payload
}

// Opcode returns the operation identified by the frame.
func (d Data) Opcode() Opcode {
	return d.opcode
}

type dataJSON struct {
	// Opcode is nil when the operation code is missing or null.
	Opcode *Opcode `json:"opcode"`
	// Emitter is nil when the sender type is missing or null.
	Emitter *emitter `json:"emitter"`
	// Metadata preserves emitter-specific JSON until its decoder is selected.
	Metadata *json.RawMessage `json:"metadata"`
	// Payload preserves operation-specific JSON until its decoder is selected.
	Payload *json.RawMessage `json:"payload"`
}

func (d dataJSON) validate() error {
	if d.Opcode == nil {
		return ErrMissingOpcode
	}
	if d.Emitter == nil {
		return ErrMissingEmitter
	}
	if d.Metadata == nil {
		return ErrMissingMetadata
	}
	if d.Payload == nil {
		return ErrMissingPayload
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
