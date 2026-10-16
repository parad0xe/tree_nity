package protocol

import (
	"bytes"
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

	data := Data{
		opcode:  *d.Opcode,
		emitter: *d.Emitter,
	}

	if err := data.validateMetadata(*d.Metadata); err != nil {
		return Data{}, err
	}
	if err := data.validatePayload(*d.Payload); err != nil {
		return Data{}, err
	}

	return data, nil
}

func (d *Data) validateMetadata(raw json.RawMessage) error {
	var tmpMetadata metadataJSON

	switch d.emitter {
	case client:
		tmpMetadata = &clientMetadataJSON{}
	case server:
		tmpMetadata = &ServerMetadataJSON{}
	default:
		return fmt.Errorf("unknown emitter: %d", d.emitter)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(tmpMetadata); err != nil {
		return err
	}

	metadata, err := tmpMetadata.toMetadata()
	if err != nil {
		return err
	}

	d.metadata = metadata
	return nil
}

func (d *Data) validatePayload(raw json.RawMessage) error {
	var tmpPayload payloadJSON

	switch d.opcode {
	case list_topic:
		tmpPayload = &listTopicPayloadJSON{}
	case create_topic:
		tmpPayload = &createTopicPayloadJSON{}
	case consumer_info:
		tmpPayload = &consumerInfoPayloadJSON{}
	case topic_subsription:
		tmpPayload = &topicSubscriptionPayloadJSON{}
	case aknowledgement:
		tmpPayload = &acknowledgementPayloadJSON{}
	case check_topic:
		tmpPayload = &checkTopicPayloadJSON{}
	case produce_message:
		tmpPayload = &produceMessagePayloadJSON{}
	case consumer_disconnect:
		tmpPayload = &consumerDisconnectPayloadJSON{}
	case consumer_heartbeat:
		tmpPayload = &consumerHeartbeatPayloadJSON{}
	default:
		return fmt.Errorf("unknown opcode: %d", d.opcode)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(tmpPayload); err != nil {
		return err
	}

	payload, err := tmpPayload.toPayload()
	if err != nil {
		return err
	}
	if err := payload.parse(); err != nil {
		return err
	}

	d.payload = payload
	return nil
}
