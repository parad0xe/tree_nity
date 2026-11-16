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

	return response.SendToClient(
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

// CheckTopic replies with existence or rejection status; ctx bounds delivery.
func (m *Manager) CheckTopic(ctx context.Context, metadata protocol.ClientMetadata, payload protocol.CheckTopicPayload) error {
	topicID := id.ID(payload.Topic())
	if err := topicID.Validate(); err != nil {
		return response.SendToClient(
			ctx,
			metadata,
			protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.InvalidRequest, err.Error()),
		)
	}

	m.mu.RLock()
	_, exists := m.topicMap.Get(topicID)
	m.mu.RUnlock()

	if !exists {
		return response.SendToClient(
			ctx,
			metadata,
			protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.TopicNotFound, "topic does not exist"),
		)
	}

	return response.SendToClient(
		ctx,
		metadata,
		protocol.CheckTopicReply,
		protocol.NewCheckTopicReplyPayload(),
	)
}

// ProduceMessage forwards a valid message to its topic, replying only on rejection.
func (m *Manager) ProduceMessage(ctx context.Context, metadata protocol.ClientMetadata, payload protocol.ProduceMessagePayload) error {
	topicID := id.ID(payload.Topic())
	message := payload.Message()
	err := topicID.Validate()
	if err == nil && len(message.Key())+len(message.Value()) > protocol.MessageSize {
		err = protocol.ErrMessageTooLarge
	}
	if err != nil {
		return response.SendToClient(
			ctx,
			metadata,
			protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.InvalidRequest, err.Error()),
		)
	}

	m.mu.RLock()
	topic, exists := m.topicMap.Get(topicID)
	m.mu.RUnlock()
	if !exists {
		return response.SendToClient(
			ctx,
			metadata,
			protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.TopicNotFound, "topic does not exist"),
		)
	}

	return topic.ProduceMessage(ctx, message)
}
