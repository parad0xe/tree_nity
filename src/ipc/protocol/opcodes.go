package protocol

type op int

const (
	list_topic = iota
	create_topic
	consumer_info
	topic_subsription
	aknowledgement
	check_topic
	produce_message
	consumer_disconnect
	consumer_heartbeat
)
