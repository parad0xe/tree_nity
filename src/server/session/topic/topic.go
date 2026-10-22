package topic

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"tree_nity/src/ipc/protocol"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils"
	"tree_nity/src/server/utils/hashmap"
)

// Topic holds the message and consumer indexes and lifecycle state for a topic.
// It is intended to own one message-processing goroutine and two goroutines
// per active consumer connection: heartbeat monitoring and message delivery.
// A Topic must not be copied after initialization.
type Topic struct {
	mu                sync.Mutex
	waitGroup         sync.WaitGroup
	messageOffsetTrie *MessageOffsetTrie
	consumerTrie      *ConsumerTrie
	consumerMap       *hashmap.HashMap[id.ID, *consumer.Consumer]
	newMessageCond    *sync.Cond
}

// NewTopic returns a topic with initialized indexes.
// ctx must be non-nil and is reserved for the message-processing loop,
// which remains to be implemented.
func NewTopic(ctx context.Context) *Topic {
	t := &Topic{
		messageOffsetTrie: NewMessageOffsetTrie(),
		consumerTrie:      NewConsumerTrie(),
		consumerMap:       hashmap.NewHashMap[id.ID, *consumer.Consumer](),
	}
	t.newMessageCond = sync.NewCond(&t.mu)
	// TODO: start a single run goroutine when message processing is implemented.
	return t
}

// UpsertConsumer adds or updates a consumer's subscription, metadata, and connection.
// ctx must be non-nil. Each call starts two workers with a child context of ctx.
// The topic copies the metadata and shares the supplied connection. A successful
// update cancels the previous workers, which keep their original connection.
// A worker returning cancels its sibling through their shared child context.
// It returns ErrUninitialized for an unusable topic, ErrInvalidConsumer for a nil
// consumer, invalid ID, or invalid connection. An already canceled or expired ctx
// returns its context error before either index is changed.
// Failed index updates trigger rollback before returning their errors.
func (t *Topic) UpsertConsumer(ctx context.Context, c *consumer.Consumer) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConsumer, err)
	}

	newConsumer := *c
	consumerCTX, cancel := context.WithCancel(ctx)

	t.mu.Lock()

	oldConnection, err := t.registerConsumer(consumerCTX, cancel, &newConsumer)
	if err == nil {
		t.startConsumerWorkers(consumerCTX, &newConsumer)
	}

	t.mu.Unlock()

	if err != nil {
		cancel()
		return err
	}

	if oldConnection != nil {
		oldConnection.Close()
	}

	return nil
}

// registerConsumer updates the subscription and consumer indexes.
// The new consumer must already be validated.
// The caller must hold t.mu and start the new workers before unlocking.
// Only successful updates return the previous connection for closure.
// The new consumer keeps its connection even when a later update
// replaces the stored consumer.
func (t *Topic) registerConsumer(ctx context.Context, cancel context.CancelFunc, newConsumer *consumer.Consumer) (*consumer.Connection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	consumerID := newConsumer.Info.ID()
	if storedConsumer, exists := t.consumerMap.Get(consumerID); exists {
		if err := storedConsumer.Validate(); err != nil {
			return nil, fmt.Errorf("%w: stored consumer %q: %v", ErrInvalidConsumer, consumerID, err)
		}
		return t.updateConsumer(consumerID, storedConsumer, newConsumer, cancel)
	}

	if err := t.insertConsumer(consumerID, newConsumer, cancel); err != nil {
		return nil, err
	}

	return nil, nil
}

func (t *Topic) insertConsumer(consumerID id.ID, newConsumer *consumer.Consumer, cancel context.CancelFunc) error {
	prefix := newConsumer.Info.Prefix()

	if err := t.consumerTrie.Subscribe(prefix, consumerID); err != nil {
		return fmt.Errorf("subscribe consumer %q: %w", consumerID, err)
	}

	storedConsumer := *newConsumer
	if err := t.consumerMap.Insert(consumerID, &storedConsumer); err != nil {
		rollbackErr := t.consumerTrie.Unsubscribe(prefix, consumerID)
		return fmt.Errorf("insert consumer %q: %w", consumerID, errors.Join(err, rollbackErr))
	}

	newConsumer.Connection.Cancel = cancel
	return nil
}

func (t *Topic) updateConsumer(consumerID id.ID, storedConsumer *consumer.Consumer, newConsumer *consumer.Consumer, cancel context.CancelFunc) (*consumer.Connection, error) {
	if storedConsumer.Connection == newConsumer.Connection {
		return nil, fmt.Errorf("%w: connection already attached for consumer %q", ErrInvalidConsumer, consumerID)
	}

	if err := t.updateConsumerSubscription(consumerID, storedConsumer, newConsumer); err != nil {
		return nil, err
	}

	oldConnection := storedConsumer.Connection

	newConsumer.Connection.Cancel = cancel
	*storedConsumer = *newConsumer

	return oldConnection, nil
}

func (t *Topic) updateConsumerSubscription(consumerID id.ID, storedConsumer *consumer.Consumer, newConsumer *consumer.Consumer) error {
	oldPrefix := storedConsumer.Info.Prefix()
	newPrefix := newConsumer.Info.Prefix()

	if oldPrefix == newPrefix {
		return nil
	}

	if err := t.consumerTrie.Subscribe(newPrefix, consumerID); err != nil {
		return fmt.Errorf("subscribe consumer %q: %w", consumerID, err)
	}

	if err := t.consumerTrie.Unsubscribe(oldPrefix, consumerID); err != nil {
		rollbackErr := t.consumerTrie.Unsubscribe(newPrefix, consumerID)
		return fmt.Errorf("move consumer %q: %w", consumerID, errors.Join(err, rollbackErr))
	}

	return nil
}

func (t *Topic) startConsumerWorkers(ctx context.Context, newConsumer *consumer.Consumer) {
	t.waitGroup.Add(2)

	workDone := func() {
		defer t.waitGroup.Done()
		newConsumer.Connection.Cancel()
		_ = t.RemoveConsumer(newConsumer)
	}

	go func() {
		defer workDone()
		t.listenHeartbeat(ctx, newConsumer)
	}()

	go func() {
		defer workDone()
		t.deliverMessages(ctx, newConsumer)
	}()
}

// RemoveConsumer detaches a consumer from both indexes and requests its routines to stop.
// It returns ErrUninitialized for an unusable topic or ErrInvalidConsumer for an
// invalid or stored consumer.
// Failed removals trigger rollback and leave the connection unclosed.
func (t *Topic) RemoveConsumer(c *consumer.Consumer) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConsumer, err)
	}

	t.mu.Lock()
	connection, err := t.unregisterConsumer(c)
	t.mu.Unlock()

	if err != nil {
		return err
	}
	if connection != nil {
		connection.Close()
	}

	return nil
}

// unregisterConsumer removes a consumer from both indexes.
// The caller must hold t.mu.
// Only the connection currently registered for the consumer may be removed.
// It returns the removed connection for closure after unlocking.
func (t *Topic) unregisterConsumer(c *consumer.Consumer) (*consumer.Connection, error) {
	consumerID := c.Info.ID()

	storedConsumer, exists := t.consumerMap.Get(consumerID)
	if !exists {
		return nil, nil
	}
	if err := storedConsumer.Validate(); err != nil {
		return nil, fmt.Errorf("%w: stored consumer %q: %v", ErrInvalidConsumer, consumerID, err)
	}
	if storedConsumer.Connection != c.Connection {
		return nil, nil
	}

	prefix := storedConsumer.Info.Prefix()

	if err := t.consumerTrie.Unsubscribe(prefix, consumerID); err != nil {
		return nil, fmt.Errorf("unsubscribe consumer %q: %w", consumerID, err)
	}

	if err := t.consumerMap.Remove(consumerID); err != nil {
		rollbackErr := t.consumerTrie.Subscribe(prefix, consumerID)
		return nil, fmt.Errorf("remove consumer %q: %w", consumerID, errors.Join(err, rollbackErr))
	}

	return storedConsumer.Connection, nil
}

// ProduceMessage is the entry point for submitting a new message to the topic loop.
func (t *Topic) ProduceMessage(message protocol.Message) {
	// TODO: enqueue the message while holding newMessageCond.L, then signal
	// newMessageCond so run can process it.
}

func (t *Topic) validate() error {
	if t == nil || t.consumerMap == nil || t.consumerTrie == nil {
		return ErrUninitialized
	}

	return nil
}

// run is the single topic goroutine's entry point for receiving new messages.
func (t *Topic) run() {
	// TODO: wait for pending messages on newMessageCond, store and index them,
	// and select matching consumers with consumerTrie. Release shared-state
	// locks before forwarding each message to their Connection.MessageChan.
	// Wake condition waits on cancellation and make channel sends cancellable,
	// then cancel every attached consumer's Connection and wait for both loops.
}

// deliverMessages is the sending goroutine's entry point for one subscribed consumer.
func (t *Topic) deliverMessages(ctx context.Context, consumer *consumer.Consumer) {
	// TODO: replay messages from the consumer's stored offset, then receive
	// targeted messages from Connection.MessageChan and send them through its IPC.
	// Process Connection.AcknowledgedChan using the next offset to consume.
	// Keep using this worker's connection. Read or update current metadata through
	// consumerMap under t.mu, only while this connection is still current.
	// Stop on consumer removal, disconnection, or ctx cancellation.

	<-ctx.Done()
}

// listenHeartbeat is the heartbeat goroutine's entry point for one subscribed consumer.
func (t *Topic) listenHeartbeat(ctx context.Context, consumer *consumer.Consumer) {
	// TODO: reset the consumer's timeout on each heartbeat and stop the
	// connection if that timeout expires.

	for {
		select {
		case <-ctx.Done():
			return

		case _, ok := <-consumer.Connection.HeartbeatChan:
			if !ok {
				return
			}
		}
	}
}
