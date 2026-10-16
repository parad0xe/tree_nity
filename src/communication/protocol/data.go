package protocol

import (
	"encoding/json"
	"fmt"
)

type Data struct {
	opcode   op
	emitter  emitter
	metadata Metadata
	payload  Payload
}

type dataJSON struct {
	Opcode   *op              `json:"opcode"`
	Emitter  *emitter         `json:"emitter"`
	Metadata *json.RawMessage `json:"metadata"`
	Payload  *json.RawMessage `json:"payload"`
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
