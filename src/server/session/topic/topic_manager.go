package topic

import (
	"context"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/hashmap"
)

// Manager holds state for topic-management handlers, which are not implemented yet.
type Manager struct {
	consumerManager consumer.Manager
	topicMap        hashmap.HashMap[id.ID, *Topic]
	context         context.Context
}

// ListTopics is the entry point for requests to list available topics.
func (m *Manager) ListTopics(metadata protocol.ClientMetadata, payload protocol.ListTopicPayload) {
	// TODO: collect topic IDs from topicMap and reply to the client with a
	// comma-separated list of topic names.
}

// CreateTopic is the entry point for topic creation requests.
func (m *Manager) CreateTopic(metadata protocol.ClientMetadata, payload protocol.CreateTopicPayload) {
	// TODO: validate the requested topic ID, reject duplicates, initialize the
	// topic in topicMap, and send the creation confirmation to the client.
}

// NewSubscription is the entry point for subscription requests.
func (m *Manager) NewSubscription(metadata protocol.ClientMetadata, payload protocol.TopicSubscriptionPayload) {
	// TODO: check the topic, resolve the consumer and update its metadata,
	// detach any previous topic subscription, then call Topic.UpsertConsumer.
}

// CheckTopic is the entry point for topic existence checks.
func (m *Manager) CheckTopic(metadata protocol.ClientMetadata, payload protocol.CheckTopicPayload) {
	// TODO: look up the requested topic in topicMap and reply to the client
	// with its existence status before message production begins.
}

// ProduceMessage is the entry point for publishing messages to a topic.
func (m *Manager) ProduceMessage(metadata protocol.ClientMetadata, payload protocol.ProduceMessagePayload) {
	// TODO: reject missing topics, forward the message to the topic for storage
	// and indexing, and notify its subscribed consumers.
}
