package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Payload holds operation-specific data and converts it to JSON for its opcode.
type Payload interface {
	toJSON(opcode Opcode) (payloadJSON, error)
	validate() error
}

type payloadJSON interface {
	validate() error
	toPayload() (Payload, error)
}

func newPayloadJSON(opcode Opcode, raw json.RawMessage) (payloadJSON, error) {
	var tmpPayload payloadJSON

	switch opcode {
	case ListTopic:
		tmpPayload = &listTopicPayloadJSON{}
	case CreateTopic:
		tmpPayload = &createTopicPayloadJSON{}
	case ConsumerInfo:
		tmpPayload = &consumerInfoPayloadJSON{}
	case TopicSubscription:
		tmpPayload = &topicSubscriptionPayloadJSON{}
	case Acknowledgement:
		tmpPayload = &acknowledgementPayloadJSON{}
	case CheckTopic:
		tmpPayload = &checkTopicPayloadJSON{}
	case ProduceMessage:
		tmpPayload = &produceMessagePayloadJSON{}
	case ConsumerDisconnect:
		tmpPayload = &consumerDisconnectPayloadJSON{}
	case ConsumerHeartbeat:
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

func (p *ListTopicPayload) toJSON(opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if opcode != ListTopic {
		return nil, fmt.Errorf("%w for opcode %d: %T", ErrInvalidPayload, opcode, p)
	}
	return &listTopicPayloadJSON{}, nil
}

func (p *ListTopicPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

type listTopicPayloadJSON struct{}

func (p listTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ListTopicPayload{}, nil
}

func (p listTopicPayloadJSON) validate() error {
	return nil
}

// CreateTopicPayload represents a topic creation operation with no payload fields.
type CreateTopicPayload struct{}

func (p *CreateTopicPayload) toJSON(opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if opcode != CreateTopic {
		return nil, fmt.Errorf("%w for opcode %d: %T", ErrInvalidPayload, opcode, p)
	}
	return &createTopicPayloadJSON{}, nil
}

func (p *CreateTopicPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

type createTopicPayloadJSON struct{}

func (p createTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CreateTopicPayload{}, nil
}

func (p createTopicPayloadJSON) validate() error {
	return nil
}

// ConsumerInfoPayload represents a consumer information operation with no payload fields.
type ConsumerInfoPayload struct{}

func (p *ConsumerInfoPayload) toJSON(opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if opcode != ConsumerInfo {
		return nil, fmt.Errorf("%w for opcode %d: %T", ErrInvalidPayload, opcode, p)
	}
	return &consumerInfoPayloadJSON{}, nil
}

func (p *ConsumerInfoPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

type consumerInfoPayloadJSON struct{}

func (p consumerInfoPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerInfoPayload{}, nil
}

func (p consumerInfoPayloadJSON) validate() error {
	return nil
}

// TopicSubscriptionPayload represents a subscription operation with no payload fields.
type TopicSubscriptionPayload struct{}

func (p *TopicSubscriptionPayload) toJSON(opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if opcode != TopicSubscription {
		return nil, fmt.Errorf("%w for opcode %d: %T", ErrInvalidPayload, opcode, p)
	}
	return &topicSubscriptionPayloadJSON{}, nil
}

func (p *TopicSubscriptionPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

type topicSubscriptionPayloadJSON struct{}

func (p topicSubscriptionPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &TopicSubscriptionPayload{}, nil
}

func (p topicSubscriptionPayloadJSON) validate() error {
	return nil
}

// AcknowledgementPayload represents an acknowledgement with no payload fields.
type AcknowledgementPayload struct{}

func (p *AcknowledgementPayload) toJSON(opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if opcode != Acknowledgement {
		return nil, fmt.Errorf("%w for opcode %d: %T", ErrInvalidPayload, opcode, p)
	}
	return &acknowledgementPayloadJSON{}, nil
}

func (p *AcknowledgementPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

type acknowledgementPayloadJSON struct{}

func (p acknowledgementPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &AcknowledgementPayload{}, nil
}

func (p acknowledgementPayloadJSON) validate() error {
	return nil
}

// CheckTopicPayload represents a topic check operation with no payload fields.
type CheckTopicPayload struct{}

func (p *CheckTopicPayload) toJSON(opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if opcode != CheckTopic {
		return nil, fmt.Errorf("%w for opcode %d: %T", ErrInvalidPayload, opcode, p)
	}
	return &checkTopicPayloadJSON{}, nil
}

func (p *CheckTopicPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

type checkTopicPayloadJSON struct{}

func (p checkTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CheckTopicPayload{}, nil
}

func (p checkTopicPayloadJSON) validate() error {
	return nil
}

// ProduceMessagePayload carries a message and its raw flag.
type ProduceMessagePayload struct {
	raw     bool
	message Message
}

func (p *ProduceMessagePayload) toJSON(opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if opcode != ProduceMessage {
		return nil, fmt.Errorf("%w for opcode %d: %T", ErrInvalidPayload, opcode, p)
	}
	message, err := p.message.toJSON()
	if err != nil {
		return nil, err
	}
	return &produceMessagePayloadJSON{Raw: &p.raw, Message: &message}, nil
}

func (p *ProduceMessagePayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

type produceMessagePayloadJSON struct {
	// Raw must be present even when its value is false.
	Raw *bool `json:"raw"`
	// Message must be present and pass key, value, and size validation.
	Message *messageJSON `json:"message"`
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

func (p produceMessagePayloadJSON) validate() error {
	if p.Raw == nil {
		return ErrMissingRaw
	}
	if p.Message == nil {
		return ErrMissingMessage
	}
	return nil
}

// ConsumerDisconnectPayload represents a disconnection with no payload fields.
type ConsumerDisconnectPayload struct{}

func (p *ConsumerDisconnectPayload) toJSON(opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if opcode != ConsumerDisconnect {
		return nil, fmt.Errorf("%w for opcode %d: %T", ErrInvalidPayload, opcode, p)
	}
	return &consumerDisconnectPayloadJSON{}, nil
}

func (p *ConsumerDisconnectPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

type consumerDisconnectPayloadJSON struct{}

func (p consumerDisconnectPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerDisconnectPayload{}, nil
}

func (p consumerDisconnectPayloadJSON) validate() error {
	return nil
}

// ConsumerHeartbeatPayload represents a heartbeat with no payload fields.
type ConsumerHeartbeatPayload struct{}

func (p *ConsumerHeartbeatPayload) toJSON(opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if opcode != ConsumerHeartbeat {
		return nil, fmt.Errorf("%w for opcode %d: %T", ErrInvalidPayload, opcode, p)
	}
	return &consumerHeartbeatPayloadJSON{}, nil
}

func (p *ConsumerHeartbeatPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

type consumerHeartbeatPayloadJSON struct{}

func (p consumerHeartbeatPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerHeartbeatPayload{}, nil
}

func (p consumerHeartbeatPayloadJSON) validate() error {
	return nil
}
