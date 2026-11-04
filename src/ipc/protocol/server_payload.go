package protocol

import (
	"fmt"
	"unicode/utf8"

	"tree_nity/src/ipc/id"
)

// ListTopicReplyPayload carries topic names in their supplied order.
// Its JSON representation is {"topics":["<name>"]}; an empty list is encoded as [].
// Topic names follow the same format as CreateTopicPayload.
type ListTopicReplyPayload struct {
	topics []string
}

// NewListTopicReplyPayload returns a response containing a copy of topics.
// Nil and empty slices both represent an empty topic list.
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

// CreateTopicReplyPayload confirms topic creation with an empty JSON object.
// A duplicate topic is reported with ErrorReply and TopicExists instead.
type CreateTopicReplyPayload struct{}

// NewCreateTopicReplyPayload returns an empty topic creation response.
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
// JSON fields appear in the order client, topic, offset, prefix, ipc. All fields
// are required; prefix and ipc may be empty. An unknown consumer is reported with
// ErrorReply and ClientNotFound instead.
type ConsumerInfoReplyPayload struct {
	client string
	topic  string
	offset int32
	prefix string
	ipc    string
}

// NewConsumerInfoReplyPayload returns information about the named consumer.
// Client names follow ClientMetadata.ClientID's format, and topic names follow
// CreateTopicPayload's format. NewData and Encode reject negative offsets and
// invalid UTF-8 in prefix or ipc.
func NewConsumerInfoReplyPayload(client, topic string, offset int32, prefix, ipc string) *ConsumerInfoReplyPayload {
	return &ConsumerInfoReplyPayload{client: client, topic: topic, offset: offset, prefix: prefix, ipc: ipc}
}

// Client returns the name of the queried consumer.
func (p ConsumerInfoReplyPayload) Client() string {
	return p.client
}

// Topic returns the topic to which the consumer is subscribed.
func (p ConsumerInfoReplyPayload) Topic() string {
	return p.topic
}

// Offset returns the consumer's next queue offset to consume.
func (p ConsumerInfoReplyPayload) Offset() int32 {
	return p.offset
}

// Prefix returns the consumer's key filter, or an empty string for all messages.
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

// TopicSubscriptionReplyPayload confirms a subscription with an empty JSON object.
// Rejections are reported with ErrorReply instead.
type TopicSubscriptionReplyPayload struct{}

// NewTopicSubscriptionReplyPayload returns an empty subscription response.
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

// CheckTopicReplyPayload confirms that a topic exists with an empty JSON object.
// A missing topic is reported with ErrorReply and TopicNotFound instead.
type CheckTopicReplyPayload struct{}

// NewCheckTopicReplyPayload returns an empty topic check response.
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

// NewErrorReplyPayload returns a payload describing a rejected request.
func NewErrorReplyPayload(kind ErrorKind, reason string) *ErrorReplyPayload {
	return &ErrorReplyPayload{kind: kind, reason: reason}
}

// Kind returns the reason's machine-readable category.
func (p ErrorReplyPayload) Kind() ErrorKind {
	return p.kind
}

// Reason returns the diagnostic for the client to display.
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

// MessageDeliveryPayload carries a message and its server-assigned queue offset.
// Its JSON message contains base64 key and value strings and a required offset,
// including when the offset is zero. The decoded key and value share MessageSize.
type MessageDeliveryPayload struct {
	message Message
}

// NewMessageDeliveryPayload returns a delivery containing message at offset.
// The supplied message is unchanged. NewData and Encode reject negative offsets
// and messages exceeding MessageSize bytes.
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

// ServerShutdownPayload signals server shutdown with an empty JSON object.
// Consumers interpret this sentinel as a successful end of their subscription.
type ServerShutdownPayload struct{}

// NewServerShutdownPayload returns an empty server shutdown sentinel.
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
