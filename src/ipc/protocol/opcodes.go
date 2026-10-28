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
// TODO: add server opcodes starting at zero when server payloads are defined.
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
		// No server opcodes are defined yet.
	default:
		return fmt.Errorf("%w: %d", ErrUnknownEmitter, emitter)
	}
	return fmt.Errorf("%w for emitter %d: %d", ErrUnknownOpcode, emitter, o)
}
