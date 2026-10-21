package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Payload holds operation-specific data decoded from a frame.
type Payload interface{}

type payloadJSON interface {
	validate() error
	toPayload() (Payload, error)
}

func newPayloadJSON(opcode op, raw json.RawMessage) (payloadJSON, error) {
	var tmpPayload payloadJSON

	switch opcode {
	case listTopic:
		tmpPayload = &listTopicPayloadJSON{}
	case createTopic:
		tmpPayload = &createTopicPayloadJSON{}
	case consumerInfo:
		tmpPayload = &consumerInfoPayloadJSON{}
	case topicSubscription:
		tmpPayload = &topicSubscriptionPayloadJSON{}
	case acknowledgement:
		tmpPayload = &acknowledgementPayloadJSON{}
	case checkTopic:
		tmpPayload = &checkTopicPayloadJSON{}
	case produceMessage:
		tmpPayload = &produceMessagePayloadJSON{}
	case consumerDisconnect:
		tmpPayload = &consumerDisconnectPayloadJSON{}
	case consumerHeartbeat:
		tmpPayload = &consumerHeartbeatPayloadJSON{}
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownOpcode, opcode)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(tmpPayload); err != nil {
		return nil, err
	}

	return tmpPayload, nil
}

// ListTopicPayload represents a list topic operation with no payload fields.
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

// CreateTopicPayload represents a topic creation operation with no payload fields.
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

// ConsumerInfoPayload represents a consumer information operation with no payload fields.
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

// TopicSubscriptionPayload represents a subscription operation with no payload fields.
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

// AcknowledgementPayload represents an acknowledgement with no payload fields.
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

// CheckTopicPayload represents a topic check operation with no payload fields.
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

// ProduceMessagePayload carries a message and its raw flag.
type ProduceMessagePayload struct {
	raw     bool
	message Message
}

type produceMessagePayloadJSON struct {
	// Raw must be present even when its value is false.
	Raw *bool `json:"raw"`
	// Message must be present and pass key, value, and size validation.
	Message *messageJSON `json:"message"`
}

func (p produceMessagePayloadJSON) validate() error {
	if p.Raw == nil {
		return ErrMissingRaw
	}
	if p.Message == nil {
		return ErrMissingMessage
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

// ConsumerDisconnectPayload represents a disconnection with no payload fields.
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

// ConsumerHeartbeatPayload represents a heartbeat with no payload fields.
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
