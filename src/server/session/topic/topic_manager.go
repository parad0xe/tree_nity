package topic

import (
	"context"
	"errors"
	"sync"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
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

	topicCtx, cancel := context.WithCancel(topicCtx)
	m.mu.Lock()
	target := NewTopic(topicCtx)
	err := m.topicMap.Insert(topicID, target)
	m.mu.Unlock()
	defer func() {
		// A registered topic follows its parent context; failed insertions roll back.
		if err != nil {
			cancel()
			<-target.done
		}
	}()

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

// NewSubscription attaches and confirms a consumer unless shutdown is requested.
// ctx bounds the reply; consumerCtx covers the subscription's lifetime.
func (m *Manager) NewSubscription(ctx, consumerCtx context.Context, shutdown lifecycle.GracefulShutdown, metadata protocol.ClientMetadata, payload protocol.TopicSubscriptionPayload) error {
	if shutdown.IsRequested() {
		return response.SendToClient(ctx, metadata, protocol.ServerShutdown, protocol.NewServerShutdownPayload())
	}

	if _, err := protocol.NewData(protocol.TopicSubscription, &metadata, &payload); err != nil {
		return response.SendToClient(ctx, metadata, protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.InvalidRequest, err.Error()))
	}

	m.mu.RLock()
	target, exists := m.topicMap.Get(id.ID(payload.Topic()))
	m.mu.RUnlock()
	if !exists {
		return response.SendToClient(ctx, metadata, protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.TopicNotFound, "topic does not exist"))
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	c, previous, err := m.consumerManager.Subscribe(consumerCtx, id.ID(payload.ClientID()),
		payload.Topic(), payload.Offset(), payload.Prefix(), metadata.ClientIPC())
	if errors.Is(err, consumer.ErrAlreadyConnected) {
		return response.SendToClient(ctx, metadata, protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.ClientNameTaken, err.Error()))
	}
	if err != nil {
		return err
	}

	setupDone := make(chan struct{})
	defer close(setupDone)

	onStop := func() {
		<-setupDone
		_ = m.consumerManager.DisconnectConsumer(c)
	}

	rollback := func(err error) error {
		err = errors.Join(err, target.RemoveConsumer(c))
		if previous == nil {
			err = errors.Join(err, m.consumerManager.RemoveConsumer(c.Info.ID()))
		} else {
			topic, offset, prefix, ipcPath := previous.Topic(), previous.Offset(), previous.Prefix(), previous.IPCPath()
			err = errors.Join(err, m.consumerManager.UpdateInfo(c.Info.ID(), &topic, &offset, &prefix, &ipcPath))
		}
		return errors.Join(err, m.consumerManager.DisconnectConsumer(c))
	}

	if err := target.upsertConsumer(consumerCtx, c, onStop); err != nil {
		return rollback(err)
	}
	if err := response.SendToClient(ctx, metadata, protocol.TopicSubscriptionReply,
		protocol.NewTopicSubscriptionReplyPayload()); err != nil {
		return rollback(err)
	}

	return nil
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

// ProduceMessage forwards a decoded message and replies with acceptance or rejection.
// ctx bounds forwarding and reply delivery; internal or IPC errors are returned.
func (m *Manager) ProduceMessage(ctx context.Context, shutdown lifecycle.GracefulShutdown, metadata protocol.ClientMetadata, payload protocol.ProduceMessagePayload) error {
	if shutdown.IsRequested() {
		return response.SendToClient(ctx, metadata, protocol.ServerShutdown, protocol.NewServerShutdownPayload())
	}

	topicID := id.ID(payload.Topic())
	message := payload.Message()
	if err := topicID.Validate(); err != nil {
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

	if err := topic.ProduceMessage(ctx, message); err != nil {
		return err
	}
	return response.SendToClient(
		ctx,
		metadata,
		protocol.ProduceMessageReply,
		protocol.NewProduceMessageReplyPayload(),
	)
}
