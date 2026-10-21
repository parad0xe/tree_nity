package protocol

type op int

const (
	listTopic = iota
	createTopic
	consumerInfo
	topicSubscription
	acknowledgement
	checkTopic
	produceMessage
	consumerDisconnect
	consumerHeartbeat
)
