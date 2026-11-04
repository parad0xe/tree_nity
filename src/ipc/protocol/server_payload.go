package protocol

import (
	"fmt"
	"unicode/utf8"

	"tree_nity/src/ipc/id"
)

// ListTopicReplyPayload carries topic names in their supplied order.
type ListTopicReplyPayload struct {
	topics []string
}

// NewListTopicReplyPayload returns a response containing a copy of topics.
func NewListTopicReplyPayload(topics []string) *ListTopicReplyPayload {
	return &ListTopicReplyPayload{topics: append([]string(nil), topics...)}
}

// Topics returns a copy of the topic names in their supplied order.
func (p ListTopicReplyPayload) Topics() []string {
	return append([]string(nil), p.topics...)
}

func (p *ListTopicReplyPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != ListTopicReply {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	topics := p.topics
	if topics == nil {
		topics = []string{}
	}
	return &listTopicReplyPayloadJSON{Topics: &topics}, nil
}

func (p *ListTopicReplyPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	for i, topic := range p.topics {
		if err := id.ID(topic).Validate(); err != nil {
			return fmt.Errorf("%w: invalid topic at index %d: %q", ErrInvalidPayload, i, topic)
		}
	}
	return nil
}

// CreateTopicReplyPayload confirms topic creation.
type CreateTopicReplyPayload struct{}

// NewCreateTopicReplyPayload returns a topic creation response.
func NewCreateTopicReplyPayload() *CreateTopicReplyPayload {
	return &CreateTopicReplyPayload{}
}

func (p *CreateTopicReplyPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != CreateTopicReply {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &createTopicReplyPayloadJSON{}, nil
}

func (p *CreateTopicReplyPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

// ConsumerInfoReplyPayload describes a consumer's subscription.
type ConsumerInfoReplyPayload struct {
	client string
	topic  string
	offset int32
	prefix string
	ipc    string
}

// NewConsumerInfoReplyPayload returns a consumer information response.
func NewConsumerInfoReplyPayload(client, topic string, offset int32, prefix, ipc string) *ConsumerInfoReplyPayload {
	return &ConsumerInfoReplyPayload{client: client, topic: topic, offset: offset, prefix: prefix, ipc: ipc}
}

// Client returns the consumer's name.
func (p ConsumerInfoReplyPayload) Client() string {
	return p.client
}

// Topic returns the subscribed topic.
func (p ConsumerInfoReplyPayload) Topic() string {
	return p.topic
}

// Offset returns the next offset to consume.
func (p ConsumerInfoReplyPayload) Offset() int32 {
	return p.offset
}

// Prefix returns the key prefix; empty matches all messages.
func (p ConsumerInfoReplyPayload) Prefix() string {
	return p.prefix
}

// IPC returns the consumer's IPC endpoint.
func (p ConsumerInfoReplyPayload) IPC() string {
	return p.ipc
}

func (p *ConsumerInfoReplyPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != ConsumerInfoReply {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &consumerInfoReplyPayloadJSON{Client: &p.client, Topic: &p.topic, Offset: &p.offset, Prefix: &p.prefix, IPC: &p.ipc}, nil
}

func (p *ConsumerInfoReplyPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if err := id.ID(p.client).Validate(); err != nil {
		return fmt.Errorf("%w: invalid client %q", ErrInvalidPayload, p.client)
	}
	if err := id.ID(p.topic).Validate(); err != nil {
		return fmt.Errorf("%w: invalid topic %q", ErrInvalidPayload, p.topic)
	}
	if p.offset < 0 {
		return fmt.Errorf("%w: negative offset %d", ErrInvalidPayload, p.offset)
	}
	if !utf8.ValidString(p.prefix) {
		return fmt.Errorf("%w: prefix must contain valid UTF-8", ErrInvalidPayload)
	}
	if !utf8.ValidString(p.ipc) {
		return fmt.Errorf("%w: ipc must contain valid UTF-8", ErrInvalidPayload)
	}
	return nil
}

// TopicSubscriptionReplyPayload confirms a subscription.
type TopicSubscriptionReplyPayload struct{}

// NewTopicSubscriptionReplyPayload returns a subscription response.
func NewTopicSubscriptionReplyPayload() *TopicSubscriptionReplyPayload {
	return &TopicSubscriptionReplyPayload{}
}

func (p *TopicSubscriptionReplyPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != TopicSubscriptionReply {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &topicSubscriptionReplyPayloadJSON{}, nil
}

func (p *TopicSubscriptionReplyPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

// CheckTopicReplyPayload confirms that a topic exists.
type CheckTopicReplyPayload struct{}

// NewCheckTopicReplyPayload returns a topic check response.
func NewCheckTopicReplyPayload() *CheckTopicReplyPayload {
	return &CheckTopicReplyPayload{}
}

func (p *CheckTopicReplyPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != CheckTopicReply {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &checkTopicReplyPayloadJSON{}, nil
}

func (p *CheckTopicReplyPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

// ErrorReplyPayload carries a rejection kind and a reason for the client.
type ErrorReplyPayload struct {
	kind   ErrorKind
	reason string
}

// NewErrorReplyPayload returns an error response.
func NewErrorReplyPayload(kind ErrorKind, reason string) *ErrorReplyPayload {
	return &ErrorReplyPayload{kind: kind, reason: reason}
}

// Kind returns the rejection category.
func (p ErrorReplyPayload) Kind() ErrorKind {
	return p.kind
}

// Reason returns the diagnostic reason.
func (p ErrorReplyPayload) Reason() string {
	return p.reason
}

func (p *ErrorReplyPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != ErrorReply {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &errorReplyPayloadJSON{Kind: &p.kind, Reason: &p.reason}, nil
}

func (p *ErrorReplyPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if err := p.kind.validate(); err != nil {
		return err
	}
	if !utf8.ValidString(p.reason) {
		return fmt.Errorf("%w: error reason must contain valid UTF-8", ErrInvalidPayload)
	}
	return nil
}

// MessageDeliveryPayload carries a message and its queue offset.
type MessageDeliveryPayload struct {
	message Message
}

// NewMessageDeliveryPayload returns a delivery at offset.
func NewMessageDeliveryPayload(offset int32, message Message) *MessageDeliveryPayload {
	message.offset = offset
	return &MessageDeliveryPayload{message: message}
}

// Message returns the delivered message, including its queue offset.
func (p MessageDeliveryPayload) Message() Message {
	return p.message
}

func (p *MessageDeliveryPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != MessageDelivery {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	message, err := p.message.toJSON()
	if err != nil {
		return nil, err
	}
	return &messageDeliveryPayloadJSON{
		Message: &deliveredMessageJSON{messageJSON: message, Offset: &p.message.offset},
	}, nil
}

func (p *MessageDeliveryPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if p.message.offset < 0 {
		return fmt.Errorf("%w: negative message offset %d", ErrInvalidPayload, p.message.offset)
	}
	return p.message.validate()
}

// ServerShutdownPayload signals server shutdown.
type ServerShutdownPayload struct{}

// NewServerShutdownPayload returns a shutdown sentinel.
func NewServerShutdownPayload() *ServerShutdownPayload {
	return &ServerShutdownPayload{}
}

func (p *ServerShutdownPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != ServerShutdown {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &serverShutdownPayloadJSON{}, nil
}

func (p *ServerShutdownPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}
