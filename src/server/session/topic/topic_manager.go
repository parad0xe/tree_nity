package topic

import (
	"context"
	"errors"
	"sync"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/server/response"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/hashmap"
)

// Manager manages topics and handles client requests; it must not be copied.
type Manager struct {
	mu              sync.RWMutex
	consumerManager *consumer.Manager
	topicMap        *hashmap.HashMap[id.ID, *Topic]
}

// NewManager initializes topic and consumer state.
func NewManager() *Manager {
	return &Manager{
		consumerManager: consumer.NewManager(),
		topicMap:        hashmap.NewHashMap[id.ID, *Topic](),
	}
}

// ListTopics replies with topic names in unspecified order and returns internal or IPC errors.
// ctx bounds reply delivery and must be non-nil.
func (m *Manager) ListTopics(ctx context.Context, metadata protocol.ClientMetadata, payload protocol.ListTopicPayload) error {
	m.mu.RLock()
	topicIDs, err := m.topicMap.Keys()
	m.mu.RUnlock()

	if err != nil {
		return err
	}

	topics := make([]string, len(topicIDs))
	for i, topicID := range topicIDs {
		topics[i] = string(topicID)
	}

	return m.reply(
		ctx,
		metadata,
		protocol.ListTopicReply,
		protocol.NewListTopicReplyPayload(topics),
	)
}

// CreateTopic replies with creation or rejection status and returns internal or IPC errors.
// ctx bounds the reply; topicCtx must cover the topic's lifetime. Both must be non-nil.
func (m *Manager) CreateTopic(ctx, topicCtx context.Context, metadata protocol.ClientMetadata, payload protocol.CreateTopicPayload) error {
	topicID := id.ID(payload.Topic())
	if err := topicID.Validate(); err != nil {
		return response.SendToClient(
			ctx,
			metadata,
			protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.InvalidRequest, err.Error()),
		)
	}

	m.mu.Lock()
	err := m.topicMap.Insert(topicID, NewTopic(topicCtx))
	m.mu.Unlock()

	if err != nil {
		if errors.Is(err, hashmap.ErrKeyExists) {
			return response.SendToClient(
				ctx,
				metadata,
				protocol.ErrorReply,
				protocol.NewErrorReplyPayload(protocol.TopicExists, "topic already exists"),
			)
		}
		return err
	}

	return response.SendToClient(
		ctx,
		metadata,
		protocol.CreateTopicReply,
		protocol.NewCreateTopicReplyPayload(),
	)
}

// NewSubscription is reserved for subscription requests.
func (m *Manager) NewSubscription(metadata protocol.ClientMetadata, payload protocol.TopicSubscriptionPayload) {
	// TODO: check the topic, resolve the consumer and update its metadata,
	// detach any previous topic subscription, then call Topic.UpsertConsumer.
}

// CheckTopic is reserved for topic existence checks.
func (m *Manager) CheckTopic(metadata protocol.ClientMetadata, payload protocol.CheckTopicPayload) {
	// TODO: look up the requested topic in topicMap and reply to the client
	// with its existence status before message production begins.
}

// ProduceMessage is reserved for requests to publish messages to a topic.
func (m *Manager) ProduceMessage(metadata protocol.ClientMetadata, payload protocol.ProduceMessagePayload) {
	// TODO: reject missing topics, forward the message to the topic for storage
	// and indexing, and notify its subscribed consumers.
}
