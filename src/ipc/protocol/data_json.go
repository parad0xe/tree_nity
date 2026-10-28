package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type dataJSON struct {
	// Opcode is nil when the operation code is missing or null.
	Opcode *Opcode `json:"opcode"`
	// Emitter is nil when the sender type is missing or null.
	Emitter *Emitter `json:"emitter"`
	// Metadata preserves emitter-specific JSON until its decoder is selected.
	Metadata *json.RawMessage `json:"metadata"`
	// Payload preserves operation-specific JSON until its decoder is selected.
	Payload *json.RawMessage `json:"payload"`
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

	createPayload, ok := payloadJSONFactories[*d.Emitter][*d.Opcode]
	if !ok {
		return Data{}, fmt.Errorf("%w for emitter %d: %d", ErrUnknownOpcode, *d.Emitter, *d.Opcode)
	}
	tmpPayload := createPayload()

	decoder := json.NewDecoder(bytes.NewReader(*d.Payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(tmpPayload); err != nil {
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
