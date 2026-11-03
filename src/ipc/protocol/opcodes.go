package protocol

import "fmt"

// Opcode identifies an operation within a protocol frame's emitter-specific list.
type Opcode int

// Client opcodes identify operations emitted by clients.
const (
	// ListTopic identifies a list topic operation.
	ListTopic Opcode = iota
	// CreateTopic identifies a topic creation operation.
	CreateTopic
	// ConsumerInfo identifies a consumer information operation.
	ConsumerInfo
	// TopicSubscription identifies a topic subscription operation.
	TopicSubscription
	// Acknowledgement identifies a message acknowledgement.
	Acknowledgement
	// CheckTopic identifies a topic check operation.
	CheckTopic
	// ProduceMessage identifies a message production operation.
	ProduceMessage
	// ConsumerDisconnect identifies a consumer disconnection.
	ConsumerDisconnect
	// ConsumerHeartbeat identifies a consumer heartbeat.
	ConsumerHeartbeat
)

// Server opcodes identify operations emitted by the server.
const (
	// ListTopicReply identifies a topic list response.
	ListTopicReply Opcode = iota
	// CreateTopicReply identifies a topic creation response.
	CreateTopicReply
	// ConsumerInfoReply identifies a consumer information response.
	ConsumerInfoReply
	// TopicSubscriptionReply identifies a subscription response.
	TopicSubscriptionReply
	// CheckTopicReply identifies a topic check response.
	CheckTopicReply
	// ErrorReply identifies a rejected request response.
	ErrorReply
	// MessageDelivery identifies a message sent to a consumer.
	MessageDelivery
	// ServerShutdown identifies the server shutdown sentinel.
	ServerShutdown
)

func (o Opcode) validate(emitter Emitter) error {
	switch emitter {
	case Client:
		switch o {
		case ListTopic, CreateTopic, ConsumerInfo, TopicSubscription, Acknowledgement,
			CheckTopic, ProduceMessage, ConsumerDisconnect, ConsumerHeartbeat:
			return nil
		}
	case Server:
		switch o {
		case ListTopicReply, CreateTopicReply, ConsumerInfoReply, TopicSubscriptionReply,
			CheckTopicReply, ErrorReply, MessageDelivery, ServerShutdown:
			return nil
		}
	default:
		return fmt.Errorf("%w: %d", ErrUnknownEmitter, emitter)
	}
	return fmt.Errorf("%w for emitter %d: %d", ErrUnknownOpcode, emitter, o)
}
