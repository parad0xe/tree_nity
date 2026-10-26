package protocol

// Opcode identifies an operation in a protocol frame.
type Opcode int

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
