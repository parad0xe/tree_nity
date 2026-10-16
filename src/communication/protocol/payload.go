package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

type Payload interface{}

type payloadJSON interface {
	validate() error
	toPayload() (Payload, error)
}

func newPayloadJSON(opcode op, raw json.RawMessage) (payloadJSON, error) {
	var tmpPayload payloadJSON

	switch opcode {
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
		return nil, fmt.Errorf("unknown opcode: %d", opcode)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(tmpPayload); err != nil {
		return nil, err
	}

	return tmpPayload, nil
}

type ListTopicPayload struct{}
type listTopicPayloadJSON struct{}

func (p listTopicPayloadJSON) validate() error {
	return nil
}

func (p listTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ListTopicPayload{}, nil
}

type CreateTopicPayload struct{}
type createTopicPayloadJSON struct{}

func (p createTopicPayloadJSON) validate() error {
	return nil
}

func (p createTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CreateTopicPayload{}, nil
}

type ConsumerInfoPayload struct{}
type consumerInfoPayloadJSON struct{}

func (p consumerInfoPayloadJSON) validate() error {
	return nil
}

func (p consumerInfoPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerInfoPayload{}, nil
}

type TopicSubscriptionPayload struct{}
type topicSubscriptionPayloadJSON struct{}

func (p topicSubscriptionPayloadJSON) validate() error {
	return nil
}

func (p topicSubscriptionPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &TopicSubscriptionPayload{}, nil
}

type AcknowledgementPayload struct{}
type acknowledgementPayloadJSON struct{}

func (p acknowledgementPayloadJSON) validate() error {
	return nil
}

func (p acknowledgementPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &AcknowledgementPayload{}, nil
}

type CheckTopicPayload struct{}
type checkTopicPayloadJSON struct{}

func (p checkTopicPayloadJSON) validate() error {
	return nil
}

func (p checkTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CheckTopicPayload{}, nil
}

type ProduceMessagePayload struct {
	raw     bool
	message Message
}

type produceMessagePayloadJSON struct {
	Raw     *bool        `json:"raw"`
	Message *messageJSON `json:"message"`
}

func (p produceMessagePayloadJSON) validate() error {
	if p.Raw == nil {
		return errors.New("missing raw")
	}
	if p.Message == nil {
		return errors.New("missing message")
	}
	return nil
}

func (p produceMessagePayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}

	message, err := p.Message.toMessage()
	if err != nil {
		return nil, err
	}

	return &ProduceMessagePayload{
		raw:     *p.Raw,
		message: message,
	}, nil
}

type ConsumerDisconnectPayload struct{}
type consumerDisconnectPayloadJSON struct{}

func (p consumerDisconnectPayloadJSON) validate() error {
	return nil
}

func (p consumerDisconnectPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerDisconnectPayload{}, nil
}

type ConsumerHeartbeatPayload struct{}
type consumerHeartbeatPayloadJSON struct{}

func (p consumerHeartbeatPayloadJSON) validate() error {
	return nil
}

func (p consumerHeartbeatPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerHeartbeatPayload{}, nil
}
