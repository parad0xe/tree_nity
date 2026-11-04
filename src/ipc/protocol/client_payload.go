package protocol

import (
	"fmt"

	"tree_nity/src/ipc/id"
)

// ListTopicPayload represents a list topic operation with no payload fields.
type ListTopicPayload struct{}

// NewListTopicPayload returns an empty list topic payload.
func NewListTopicPayload() *ListTopicPayload {
	return &ListTopicPayload{}
}

func (p *ListTopicPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != ListTopic {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &listTopicPayloadJSON{}, nil
}

func (p *ListTopicPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

// CreateTopicPayload carries the name of the topic to create.
// Its JSON representation is {"topic":"<name>"}.
type CreateTopicPayload struct {
	topic string
}

// NewCreateTopicPayload returns a payload for creating topic.
// NewData and Encode reject names that do not contain 1 to 32 ASCII letters,
// digits, underscores, dots, or hyphens.
func NewCreateTopicPayload(topic string) *CreateTopicPayload {
	return &CreateTopicPayload{topic: topic}
}

// Topic returns the name of the topic to create.
func (p CreateTopicPayload) Topic() string {
	return p.topic
}

func (p *CreateTopicPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != CreateTopic {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &createTopicPayloadJSON{Topic: &p.topic}, nil
}

func (p *CreateTopicPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if err := id.ID(p.topic).Validate(); err != nil {
		return fmt.Errorf("%w: invalid topic %q", ErrInvalidPayload, p.topic)
	}
	return nil
}

// ConsumerInfoPayload represents a consumer information operation with no payload fields.
type ConsumerInfoPayload struct{}

// NewConsumerInfoPayload returns an empty consumer information payload.
func NewConsumerInfoPayload() *ConsumerInfoPayload {
	return &ConsumerInfoPayload{}
}

func (p *ConsumerInfoPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != ConsumerInfo {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
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

// NewTopicSubscriptionPayload returns an empty topic subscription payload.
func NewTopicSubscriptionPayload() *TopicSubscriptionPayload {
	return &TopicSubscriptionPayload{}
}

func (p *TopicSubscriptionPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != TopicSubscription {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
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

// NewAcknowledgementPayload returns an empty acknowledgement payload.
func NewAcknowledgementPayload() *AcknowledgementPayload {
	return &AcknowledgementPayload{}
}

func (p *AcknowledgementPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != Acknowledgement {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &acknowledgementPayloadJSON{}, nil
}

func (p *AcknowledgementPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

// CheckTopicPayload carries the name of the topic to check before producing messages.
// Its JSON representation is {"topic":"<name>"}.
type CheckTopicPayload struct {
	topic string
}

// NewCheckTopicPayload returns a payload for checking topic.
// Topic names follow the same format as CreateTopicPayload.
func NewCheckTopicPayload(topic string) *CheckTopicPayload {
	return &CheckTopicPayload{topic: topic}
}

// Topic returns the name of the topic to check.
func (p CheckTopicPayload) Topic() string {
	return p.topic
}

func (p *CheckTopicPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != CheckTopic {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &checkTopicPayloadJSON{Topic: &p.topic}, nil
}

func (p *CheckTopicPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if err := id.ID(p.topic).Validate(); err != nil {
		return fmt.Errorf("%w: invalid topic %q", ErrInvalidPayload, p.topic)
	}
	return nil
}

// ProduceMessagePayload carries a topic name and a message to produce.
// Its JSON representation contains topic and message; the input format is local
// to the client, and the server assigns the message offset.
type ProduceMessagePayload struct {
	topic   string
	message Message
}

// NewProduceMessagePayload returns a payload for producing message on topic.
// Topic names follow the same format as CreateTopicPayload.
func NewProduceMessagePayload(topic string, message Message) *ProduceMessagePayload {
	return &ProduceMessagePayload{topic: topic, message: message}
}

// Topic returns the name of the topic on which to produce the message.
func (p ProduceMessagePayload) Topic() string {
	return p.topic
}

// Message returns the message to produce.
func (p ProduceMessagePayload) Message() Message {
	return p.message
}

func (p *ProduceMessagePayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != ProduceMessage {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	message, err := p.message.toJSON()
	if err != nil {
		return nil, err
	}
	return &produceMessagePayloadJSON{Topic: &p.topic, Message: &message}, nil
}

func (p *ProduceMessagePayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if err := id.ID(p.topic).Validate(); err != nil {
		return fmt.Errorf("%w: invalid topic %q", ErrInvalidPayload, p.topic)
	}
	return nil
}

// ConsumerDisconnectPayload represents a disconnection with no payload fields.
type ConsumerDisconnectPayload struct{}

// NewConsumerDisconnectPayload returns an empty consumer disconnection payload.
func NewConsumerDisconnectPayload() *ConsumerDisconnectPayload {
	return &ConsumerDisconnectPayload{}
}

func (p *ConsumerDisconnectPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != ConsumerDisconnect {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
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

// NewConsumerHeartbeatPayload returns an empty consumer heartbeat payload.
func NewConsumerHeartbeatPayload() *ConsumerHeartbeatPayload {
	return &ConsumerHeartbeatPayload{}
}

func (p *ConsumerHeartbeatPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != ConsumerHeartbeat {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &consumerHeartbeatPayloadJSON{}, nil
}

func (p *ConsumerHeartbeatPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}
