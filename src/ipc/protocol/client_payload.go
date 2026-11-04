package protocol

import (
	"fmt"
	"unicode/utf8"

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
type CreateTopicPayload struct {
	topic string
}

// NewCreateTopicPayload returns a topic creation request.
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

// ConsumerInfoPayload identifies the subscriber to query.
type ConsumerInfoPayload struct {
	subscriber string
}

// NewConsumerInfoPayload returns an info request for subscriber.
func NewConsumerInfoPayload(subscriber string) *ConsumerInfoPayload {
	return &ConsumerInfoPayload{subscriber: subscriber}
}

// Subscriber returns the queried consumer's name.
func (p ConsumerInfoPayload) Subscriber() string {
	return p.subscriber
}

func (p *ConsumerInfoPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != ConsumerInfo {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &consumerInfoPayloadJSON{Subscriber: &p.subscriber}, nil
}

func (p *ConsumerInfoPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if err := id.ID(p.subscriber).Validate(); err != nil {
		return fmt.Errorf("%w: invalid subscriber %q", ErrInvalidPayload, p.subscriber)
	}
	return nil
}

// TopicSubscriptionPayload carries a topic, key prefix, and optional offset.
type TopicSubscriptionPayload struct {
	topic  string
	prefix string
	offset *int32
}

// NewTopicSubscriptionPayload returns a subscription request with a copy of offset.
func NewTopicSubscriptionPayload(topic, prefix string, offset *int32) *TopicSubscriptionPayload {
	payload := &TopicSubscriptionPayload{topic: topic, prefix: prefix}
	if offset != nil {
		start := *offset
		payload.offset = &start
	}
	return payload
}

// Topic returns the topic to subscribe to.
func (p TopicSubscriptionPayload) Topic() string {
	return p.topic
}

// Prefix returns the key prefix; empty matches all messages.
func (p TopicSubscriptionPayload) Prefix() string {
	return p.prefix
}

// Offset returns an offset copy; nil resumes, zero replays.
func (p TopicSubscriptionPayload) Offset() *int32 {
	if p.offset == nil {
		return nil
	}
	start := *p.offset
	return &start
}

func (p *TopicSubscriptionPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != TopicSubscription {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &topicSubscriptionPayloadJSON{Topic: &p.topic, Prefix: &p.prefix, Offset: p.offset}, nil
}

func (p *TopicSubscriptionPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if err := id.ID(p.topic).Validate(); err != nil {
		return fmt.Errorf("%w: invalid topic %q", ErrInvalidPayload, p.topic)
	}
	if !utf8.ValidString(p.prefix) {
		return fmt.Errorf("%w: prefix must contain valid UTF-8", ErrInvalidPayload)
	}
	if p.offset != nil && *p.offset < 0 {
		return fmt.Errorf("%w: negative offset %d", ErrInvalidPayload, *p.offset)
	}
	return nil
}

// AcknowledgementPayload carries the next offset to consume (received offset + 1).
type AcknowledgementPayload struct {
	offset int32
}

// NewAcknowledgementPayload returns an acknowledgement for the next offset.
func NewAcknowledgementPayload(offset int32) *AcknowledgementPayload {
	return &AcknowledgementPayload{offset: offset}
}

// Offset returns the next offset to consume.
func (p AcknowledgementPayload) Offset() int32 {
	return p.offset
}

func (p *AcknowledgementPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Client || opcode != Acknowledgement {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &acknowledgementPayloadJSON{Offset: &p.offset}, nil
}

func (p *AcknowledgementPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if p.offset < 0 {
		return fmt.Errorf("%w: negative offset %d", ErrInvalidPayload, p.offset)
	}
	return nil
}

// CheckTopicPayload carries the topic name to check.
type CheckTopicPayload struct {
	topic string
}

// NewCheckTopicPayload returns a topic check request.
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

// ProduceMessagePayload carries a topic and message; JSON omits the offset.
type ProduceMessagePayload struct {
	topic   string
	message Message
}

// NewProduceMessagePayload returns a message production request.
func NewProduceMessagePayload(topic string, message Message) *ProduceMessagePayload {
	return &ProduceMessagePayload{topic: topic, message: message}
}

// Topic returns the message's destination topic.
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

// ConsumerDisconnectPayload signals the end of a subscription.
type ConsumerDisconnectPayload struct{}

// NewConsumerDisconnectPayload returns a disconnection request.
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

// ConsumerHeartbeatPayload signals that a consumer is alive.
type ConsumerHeartbeatPayload struct{}

// NewConsumerHeartbeatPayload returns a heartbeat.
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
