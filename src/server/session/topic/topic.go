package topic

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/hashmap"
)

// Topic owns message storage and the workers for its consumers.
// A Topic must not be copied after initialization.
type Topic struct {
	mu                sync.Mutex
	waitGroup         sync.WaitGroup
	messageOffsetTrie *MessageOffsetTrie
	consumerTrie      *ConsumerTrie
	consumerMap       *hashmap.HashMap[id.ID, *subscription]
	newMessageChan    chan protocol.Message
	messages          []protocol.Message
	done              chan struct{}
	// runErr is read only after done closes.
	runErr error
}

// NewTopic initializes the indexes and starts message storage until ctx ends.
func NewTopic(ctx context.Context) *Topic {
	t := newTopic()
	go t.run(ctx)
	return t
}

func newTopic() *Topic {
	return &Topic{
		messageOffsetTrie: NewMessageOffsetTrie(),
		consumerTrie:      NewConsumerTrie(),
		consumerMap:       hashmap.NewHashMap[id.ID, *subscription](),
		newMessageChan:    make(chan protocol.Message),
		done:              make(chan struct{}),
	}
}

// UpsertConsumer registers a metadata snapshot and c's current connection.
// Registration starts two workers under non-nil ctx, cancels replaced workers,
// and closes their connection. An already stopped ctx returns its context error.
// It returns ErrUninitialized for an unusable topic or ErrInvalidConsumer for
// invalid consumers. Index failures attempt rollback; errors closing the old
// connection are returned after registration.
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

	newConsumer := &subscription{info: c.Info, connection: c.Connection()}
	if err := newConsumer.validate(); err != nil {
		return err
	}
	consumerCTX, cancel := context.WithCancel(ctx)
	newConsumer.cancel = cancel

	t.mu.Lock()

	oldConsumer, err := t.registerConsumer(consumerCTX, newConsumer)
	if err == nil {
		t.startConsumerWorkers(consumerCTX, newConsumer)
	}

	t.mu.Unlock()

	if err != nil {
		cancel()
		return err
	}

	if oldConsumer != nil {
		oldConsumer.cancel()
		if err := oldConsumer.connection.Close(); err != nil {
			return fmt.Errorf("close previous consumer connection: %w", err)
		}
	}

	return nil
}

// registerConsumer updates both indexes and returns a replaced subscription.
// newConsumer must be valid. The caller must hold t.mu and start the new
// workers before unlocking.
func (t *Topic) registerConsumer(ctx context.Context, newConsumer *subscription) (*subscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	consumerID := newConsumer.info.ID()
	if storedConsumer, exists := t.consumerMap.Get(consumerID); exists {
		if err := storedConsumer.validate(); err != nil {
			return nil, fmt.Errorf("%w: stored consumer %q: %v", ErrInvalidConsumer, consumerID, err)
		}
		return t.updateConsumer(consumerID, storedConsumer, newConsumer)
	}

	if err := t.insertConsumer(consumerID, newConsumer); err != nil {
		return nil, err
	}

	return nil, nil
}

func (t *Topic) insertConsumer(consumerID id.ID, newConsumer *subscription) error {
	prefix := newConsumer.info.Prefix()

	if err := t.consumerTrie.Subscribe(prefix, consumerID); err != nil {
		return fmt.Errorf("subscribe consumer %q: %w", consumerID, err)
	}

	storedConsumer := *newConsumer
	if err := t.consumerMap.Insert(consumerID, &storedConsumer); err != nil {
		rollbackErr := t.consumerTrie.Unsubscribe(prefix, consumerID)
		return fmt.Errorf("insert consumer %q: %w", consumerID, errors.Join(err, rollbackErr))
	}

	return nil
}

func (t *Topic) updateConsumer(consumerID id.ID, storedConsumer *subscription, newConsumer *subscription) (*subscription, error) {
	if storedConsumer.connection == newConsumer.connection {
		return nil, fmt.Errorf("%w: connection already attached for consumer %q", ErrInvalidConsumer, consumerID)
	}

	if err := t.updateConsumerSubscription(consumerID, storedConsumer, newConsumer); err != nil {
		return nil, err
	}

	oldConsumer := *storedConsumer

	*storedConsumer = *newConsumer

	return &oldConsumer, nil
}

func (t *Topic) updateConsumerSubscription(consumerID id.ID, storedConsumer *subscription, newConsumer *subscription) error {
	oldPrefix := storedConsumer.info.Prefix()
	newPrefix := newConsumer.info.Prefix()

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

// startConsumerWorkers starts two workers; either exiting cancels its sibling.
func (t *Topic) startConsumerWorkers(ctx context.Context, newConsumer *subscription) {
	t.waitGroup.Add(2)

	workDone := func() {
		defer t.waitGroup.Done()
		newConsumer.cancel()
		// Worker cleanup is best-effort; explicit removals report cleanup errors.
		_ = t.removeConsumer(newConsumer)
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

// RemoveConsumer detaches and closes c's registered connection.
// Absent subscriptions and superseded connections are ignored.
// It returns ErrUninitialized for an unusable topic or ErrInvalidConsumer for
// invalid consumers or stored subscriptions. Index failures attempt rollback;
// connection closure errors are returned after removal.
func (t *Topic) RemoveConsumer(c *consumer.Consumer) error {
	if err := t.validate(); err != nil {
		return err
	}
	if err := c.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConsumer, err)
	}

	return t.removeConsumer(&subscription{info: c.Info, connection: c.Connection()})
}

func (t *Topic) removeConsumer(c *subscription) error {
	if err := c.validate(); err != nil {
		return err
	}

	t.mu.Lock()
	removed, err := t.unregisterConsumer(c)
	t.mu.Unlock()

	if err != nil {
		return err
	}
	if removed != nil {
		removed.cancel()
		if err := removed.connection.Close(); err != nil {
			return fmt.Errorf("close consumer connection: %w", err)
		}
	}

	return nil
}

// unregisterConsumer removes a consumer from both indexes.
// The caller must hold t.mu.
// Only the connection currently registered for the consumer may be removed.
// It returns the removed subscription for cancellation and closure after unlocking.
func (t *Topic) unregisterConsumer(c *subscription) (*subscription, error) {
	consumerID := c.info.ID()

	storedConsumer, exists := t.consumerMap.Get(consumerID)
	if !exists {
		return nil, nil
	}
	if err := storedConsumer.validate(); err != nil {
		return nil, fmt.Errorf("%w: stored consumer %q: %v", ErrInvalidConsumer, consumerID, err)
	}
	if storedConsumer.connection != c.connection {
		return nil, nil
	}

	prefix := storedConsumer.info.Prefix()

	if err := t.consumerTrie.Unsubscribe(prefix, consumerID); err != nil {
		return nil, fmt.Errorf("unsubscribe consumer %q: %w", consumerID, err)
	}

	if err := t.consumerMap.Remove(consumerID); err != nil {
		rollbackErr := t.consumerTrie.Subscribe(prefix, consumerID)
		return nil, fmt.Errorf("remove consumer %q: %w", consumerID, errors.Join(err, rollbackErr))
	}

	return storedConsumer, nil
}

// ProduceMessage queues a message while ctx and the topic loop are active.
func (t *Topic) ProduceMessage(ctx context.Context, message protocol.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case t.newMessageChan <- message:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.done:
		return t.runErr
	}
}

func (t *Topic) validate() error {
	if t == nil || t.consumerMap == nil || t.consumerTrie == nil {
		return ErrUninitialized
	}

	return nil
}

// run is the single topic goroutine's entry point for receiving new messages.
func (t *Topic) run(ctx context.Context) {
	defer close(t.done)
	// TODO: add cancellable dispatch to matching consumers outside t.mu,
	// then cancel and wait for their workers on exit.
	for {
		if err := ctx.Err(); err != nil {
			t.runErr = err
			return
		}
		select {
		case <-ctx.Done():
			t.runErr = ctx.Err()
			return
		case message := <-t.newMessageChan:
			if err := t.storeMessage(message); err != nil {
				t.runErr = err
				return
			}
		}
	}
}

// storeMessage keeps the message at its indexed topic offset.
func (t *Topic) storeMessage(message protocol.Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if uint64(len(t.messages)) > uint64(^uint32(0)) {
		return fmt.Errorf("message offset %d exceeds uint32", len(t.messages))
	}
	offset := uint32(len(t.messages))
	if err := t.messageOffsetTrie.Insert(string(message.Key()), offset); err != nil {
		return fmt.Errorf("index message at offset %d: %w", offset, err)
	}
	t.messages = append(t.messages, message)
	return nil
}

// deliverMessages is the sending goroutine's entry point for one subscribed consumer.
func (t *Topic) deliverMessages(ctx context.Context, consumer *subscription) {
	// TODO: replay messages from the consumer's stored offset, then receive
	// targeted messages from Connection.MessageChan and send them through its IPC.
	// Process Connection.AcknowledgedChan using the next offset to consume.
	// Keep using this worker's connection. Read or update current metadata through
	// consumerMap under t.mu, only while this connection is still current.
	// Stop on consumer removal, disconnection, or ctx cancellation.

	select {
	case <-ctx.Done():
	case <-consumer.connection.Done():
	}
}

// listenHeartbeat is the heartbeat goroutine's entry point for one subscribed consumer.
func (t *Topic) listenHeartbeat(ctx context.Context, consumer *subscription) {
	// TODO: reset the consumer's timeout on each heartbeat and stop the
	// connection if that timeout expires.

	for {
		select {
		case <-ctx.Done():
			return

		case <-consumer.connection.Done():
			return

		case _, ok := <-consumer.connection.HeartbeatChan:
			if !ok {
				return
			}
		}
	}
}
