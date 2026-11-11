package topic

import (
	"context"
	"errors"
	"sync"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/hashmap"
)

// Manager manages topics and handles client requests; it must not be copied.
type Manager struct {
	mu              sync.RWMutex
	consumerManager *consumer.Manager
	topicMap        *hashmap.HashMap[id.ID, *Topic]
	context         context.Context
}

// NewManager initializes topic and consumer state; ctx must be non-nil.
func NewManager(ctx context.Context) *Manager {
	return &Manager{
		consumerManager: consumer.NewManager(),
		topicMap:        hashmap.NewHashMap[id.ID, *Topic](),
		context:         ctx,
	}
}

// ListTopics is reserved for requests to list available topics.
func (m *Manager) ListTopics(metadata protocol.ClientMetadata, payload protocol.ListTopicPayload) {
	// TODO: collect topic IDs from topicMap and reply to the client with a
	// comma-separated list of topic names.
}

// CreateTopic replies with creation or rejection status and returns internal or IPC errors.
func (m *Manager) CreateTopic(metadata protocol.ClientMetadata, payload protocol.CreateTopicPayload) error {
	topicID := id.ID(payload.Topic())
	if err := topicID.Validate(); err != nil {
		return m.reply(
			metadata,
			protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.InvalidRequest, err.Error()),
		)
	}

	m.mu.Lock()
	err := m.topicMap.Insert(topicID, NewTopic(m.context))
	m.mu.Unlock()

	if err != nil {
		if errors.Is(err, hashmap.ErrKeyExists) {
			return m.reply(
				metadata,
				protocol.ErrorReply,
				protocol.NewErrorReplyPayload(protocol.TopicExists, "topic already exists"),
			)
		}
		return err
	}

	return m.reply(
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

// reply sends a response to the client's IPC queue and closes the outbound handle.
func (m *Manager) reply(metadata protocol.ClientMetadata, opcode protocol.Opcode, payload protocol.Payload) error {
	data, err := protocol.NewData(
		opcode,
		protocol.NewServerMetadata(),
		payload,
	)
	if err != nil {
		return err
	}

	outbound, err := ipc.NewOutbound(metadata.ClientIPC(), protocol.Encode)
	if err != nil {
		return err
	}

	sendErr := outbound.Send(m.context, data)
	closeErr := outbound.Close()

	return errors.Join(sendErr, closeErr)
}
