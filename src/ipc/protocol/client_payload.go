package protocol

import "fmt"

// ListTopicPayload represents a list topic operation with no payload fields.
type ListTopicPayload struct{}

// NewListTopicPayload creates an empty list topic payload.
func NewListTopicPayload() *ListTopicPayload {
	return &ListTopicPayload{}
}

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

// CreateTopicPayload represents a topic creation operation with no payload fields.
type CreateTopicPayload struct{}

// NewCreateTopicPayload creates an empty topic creation payload.
func NewCreateTopicPayload() *CreateTopicPayload {
	return &CreateTopicPayload{}
}

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

// ConsumerInfoPayload represents a consumer information operation with no payload fields.
type ConsumerInfoPayload struct{}

// NewConsumerInfoPayload creates an empty consumer information payload.
func NewConsumerInfoPayload() *ConsumerInfoPayload {
	return &ConsumerInfoPayload{}
}

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

// TopicSubscriptionPayload represents a subscription operation with no payload fields.
type TopicSubscriptionPayload struct{}

// NewTopicSubscriptionPayload creates an empty topic subscription payload.
func NewTopicSubscriptionPayload() *TopicSubscriptionPayload {
	return &TopicSubscriptionPayload{}
}

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

// AcknowledgementPayload represents an acknowledgement with no payload fields.
type AcknowledgementPayload struct{}

// NewAcknowledgementPayload creates an empty acknowledgement payload.
func NewAcknowledgementPayload() *AcknowledgementPayload {
	return &AcknowledgementPayload{}
}

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

// CheckTopicPayload represents a topic check operation with no payload fields.
type CheckTopicPayload struct{}

// NewCheckTopicPayload creates an empty topic check payload.
func NewCheckTopicPayload() *CheckTopicPayload {
	return &CheckTopicPayload{}
}

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

// ProduceMessagePayload carries a message and its raw flag.
type ProduceMessagePayload struct {
	raw     bool
	message Message
}

// NewProduceMessagePayload creates a payload with a message and its raw flag.
// NewData and Encode validate the message's size and strings.
func NewProduceMessagePayload(raw bool, message Message) *ProduceMessagePayload {
	return &ProduceMessagePayload{raw: raw, message: message}
}

// Raw reports whether the message should be produced in raw mode.
func (p ProduceMessagePayload) Raw() bool {
	return p.raw
}

// Message returns the message to produce.
func (p ProduceMessagePayload) Message() Message {
	return p.message
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

// ConsumerDisconnectPayload represents a disconnection with no payload fields.
type ConsumerDisconnectPayload struct{}

// NewConsumerDisconnectPayload creates an empty consumer disconnection payload.
func NewConsumerDisconnectPayload() *ConsumerDisconnectPayload {
	return &ConsumerDisconnectPayload{}
}

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

// ConsumerHeartbeatPayload represents a heartbeat with no payload fields.
type ConsumerHeartbeatPayload struct{}

// NewConsumerHeartbeatPayload creates an empty consumer heartbeat payload.
func NewConsumerHeartbeatPayload() *ConsumerHeartbeatPayload {
	return &ConsumerHeartbeatPayload{}
}

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
