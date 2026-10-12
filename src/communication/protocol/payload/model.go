package protocol

type op int

const (
	// admin
	list_topic = iota
	create_topic
	consumer_info
	// consumer
	topic_subsription
	aknowledgement
	// producer
	check_topic
	produce_message
	// system
	consumer_disconnect
	consumer_heartbeat
)

type Payload struct {
	opcode op
}
