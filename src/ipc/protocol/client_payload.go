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

// ConsumerInfoPayload carries the subscriber whose information is requested.
// The subscriber can differ from the caller identified by ClientMetadata.ClientID.
// Its JSON representation is {"subscriber":"<name>"}.
type ConsumerInfoPayload struct {
	subscriber string
}

// NewConsumerInfoPayload returns a request for subscriber's information.
// Subscriber names follow the same format as ClientMetadata.ClientID.
func NewConsumerInfoPayload(subscriber string) *ConsumerInfoPayload {
	return &ConsumerInfoPayload{subscriber: subscriber}
}

// Subscriber returns the name of the consumer being queried.
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

// TopicSubscriptionPayload carries a topic, key prefix, and optional starting offset.
// The subscriber is identified by ClientMetadata.ClientID. An empty prefix matches
// all messages. An absent or null JSON offset resumes the stored offset, or starts
// at zero for a new subscriber; an explicit zero requests replay from the beginning.
type TopicSubscriptionPayload struct {
	topic  string
	prefix string
	offset *int32
}

// NewTopicSubscriptionPayload returns a subscription request with a copy of offset.
// Topic names follow the same format as CreateTopicPayload. NewData and Encode
// reject invalid UTF-8 prefixes and negative offsets. A nil offset requests resume.
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

// Prefix returns the key prefix to match, or an empty string for all messages.
func (p TopicSubscriptionPayload) Prefix() string {
	return p.prefix
}

// Offset returns a copy of the requested offset, or nil for resume.
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

// AcknowledgementPayload carries the next queue offset to consume.
// After receiving offset N, the consumer supplies N+1. Its JSON representation
// always includes offset, including zero. No success reply is expected.
type AcknowledgementPayload struct {
	offset int32
}

// NewAcknowledgementPayload returns a payload carrying the next offset to consume.
// NewData and Encode reject negative offsets.
func NewAcknowledgementPayload(offset int32) *AcknowledgementPayload {
	return &AcknowledgementPayload{offset: offset}
}

// Offset returns the next queue offset to consume.
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

// ConsumerDisconnectPayload ends a consumer subscription with an empty JSON object.
// ClientMetadata.ClientID identifies the subscriber. No success reply is expected.
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

// ConsumerHeartbeatPayload signals a live consumer with an empty JSON object.
// ClientMetadata.ClientID identifies the subscriber. No success reply is expected.
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
