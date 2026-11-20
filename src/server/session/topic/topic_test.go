package topic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/hashmap"
	"tree_nity/src/server/utils/trie"
)

func waitTopicWorkers(t *testing.T, topic *Topic) bool {
	t.Helper()
	done := make(chan struct{})
	go func() {
		topic.waitGroup.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(5 * time.Second):
		t.Errorf("topic workers after cancellation did not stop within 5s, want all workers stopped")
		return false
	}
}

func waitTopicLoop(t *testing.T, topic *Topic) bool {
	t.Helper()
	select {
	case <-topic.done:
		return true
	case <-time.After(5 * time.Second):
		t.Error("topic storage loop did not stop within 5s, want stopped")
		return false
	}
}

func newTestTopic(t *testing.T) (*Topic, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	topic := NewTopic(ctx)
	t.Cleanup(func() {
		cancel()
		waitTopicLoop(t, topic)
		waitTopicWorkers(t, topic)
	})
	return topic, ctx, cancel
}

func connectedTopicConsumer(t *testing.T, prefix, suffix string) *consumer.Consumer {
	t.Helper()
	ipcPath := fmt.Sprintf("/topic.%d.%s.%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "."), suffix)
	receiver, err := mqueue.NewReceiver(ipcPath)
	if err != nil {
		t.Errorf("NewReceiver(%q) error = %v, want nil", ipcPath, err)
		return nil
	}
	t.Cleanup(func() {
		if err := receiver.Drop(); err != nil {
			t.Errorf("Drop(%q) error = %v, want nil", ipcPath, err)
		}
	})
	manager := consumer.NewManager()
	consumerID := id.ID("consumer-1")
	if err := manager.AddConsumer(consumerID, "events", 0, prefix, ipcPath); err != nil {
		t.Errorf("AddConsumer(%q, events, 0, %q, %q) error = %v, want nil", consumerID, prefix, ipcPath, err)
		return nil
	}
	c, err := manager.GetConsumer(consumerID)
	if err != nil {
		t.Errorf("GetConsumer(%q) error = %v, want nil", consumerID, err)
		return nil
	}
	t.Cleanup(func() {
		if err := manager.DisconnectConsumer(c); err != nil {
			t.Errorf("DisconnectConsumer(%q) error = %v, want nil", consumerID, err)
		}
	})
	if err := manager.ConnectConsumer(context.Background(), c); err != nil {
		t.Errorf("ConnectConsumer(%q) to %q error = %v, want nil", consumerID, ipcPath, err)
		return nil
	}
	return c
}

// TestTopicCancellationStopsLoop checks rollback without stopping the parent context.
func TestTopicCancellationStopsLoop(t *testing.T) {
	parent := t.Context()
	ctx, cancel := context.WithCancel(parent)
	topic := NewTopic(ctx)
	t.Cleanup(func() {
		cancel()
		waitTopicLoop(t, topic)
	})
	cancel()
	if !waitTopicLoop(t, topic) {
		return
	}
	if !errors.Is(topic.runErr, context.Canceled) {
		t.Errorf("topic loop after cancellation error = %v, want %v", topic.runErr, context.Canceled)
	}
	if err := parent.Err(); err != nil {
		t.Errorf("parent context after topic cancellation error = %v, want nil", err)
	}
}

// TestTopicProduceMessageStoresInOrder checks empty, minimal and large streams.
func TestTopicProduceMessageStoresInOrder(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "empty"},
		{name: "single message", count: 1},
		{name: "many messages", count: 256},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, _, stop := newTestTopic(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for i := 0; i < tt.count; i++ {
				if err := topic.ProduceMessage(ctx, protocol.NewMessage([]byte{0, 255}, []byte{byte(i)})); err != nil {
					t.Errorf("ProduceMessage(sequence %d of %d) error = %v, want nil", i, tt.count, err)
					return
				}
			}
			stop()
			if !waitTopicLoop(t, topic) {
				return
			}
			if len(topic.messages) != tt.count {
				t.Errorf("stored message count = %d, want %d", len(topic.messages), tt.count)
			}
			for i, message := range topic.messages {
				if !bytes.Equal(message.Key(), []byte{0, 255}) || !bytes.Equal(message.Value(), []byte{byte(i)}) {
					t.Errorf("stored message at offset %d = key %x, value %x, want key %x, value %x", i, message.Key(), message.Value(), []byte{0, 255}, []byte{byte(i)})
				}
			}
			wantOffsets := make([]uint32, tt.count)
			for i := range wantOffsets {
				wantOffsets[i] = uint32(i)
			}
			offsets, err := topic.messageOffsetTrie.Search(string([]byte{0, 255}), 0)
			if err != nil || !slices.Equal(offsets, wantOffsets) {
				t.Errorf("Search(binary key, 0) after %d messages = %v, %v, want %v and nil", tt.count, offsets, err, wantOffsets)
			}
		})
	}
}

// TestTopicProduceMessageRejectsStoppedContexts checks request cancellation.
func TestTopicProduceMessageRejectsStoppedContexts(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, expireCancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer expireCancel()
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{name: "canceled", ctx: canceled, wantErr: context.Canceled},
		{name: "expired", ctx: expired, wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, _, _ := newTestTopic(t)
			if err := topic.ProduceMessage(tt.ctx, protocol.NewMessage(nil, nil)); !errors.Is(err, tt.wantErr) {
				t.Errorf("ProduceMessage(empty) with %s context error = %v, want %v", tt.name, err, tt.wantErr)
			}
		})
	}
}

// produceSelectContext signals when ProduceMessage evaluates its blocking select.
type produceSelectContext struct {
	context.Context
	selected chan struct{}
	once     sync.Once
}

func (c *produceSelectContext) Done() <-chan struct{} {
	done := c.Context.Done()
	c.once.Do(func() { close(c.selected) })
	return done
}

// TestTopicProduceMessageCancelsWithoutReceiver checks cancellation inside the select.
func TestTopicProduceMessageCancelsWithoutReceiver(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	// Isolate the send without starting the normal topic receiver.
	topic := newTopic()
	observed := &produceSelectContext{Context: ctx, selected: make(chan struct{})}
	result := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("producer goroutine did not stop within 5s after cancellation, want stopped")
		}
	})
	go func() {
		defer close(finished)
		result <- topic.ProduceMessage(observed, protocol.NewMessage(nil, nil))
	}()
	select {
	case <-observed.selected:
	case <-ctx.Done():
		t.Errorf("ProduceMessage(empty) did not reach its select: %v, want a waiting send", ctx.Err())
		return
	}
	select {
	case err := <-result:
		t.Errorf("ProduceMessage(empty) without a receiver returned %v before cancellation, want a waiting send", err)
		return
	default:
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("ProduceMessage(empty) without a receiver after cancellation error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Error("ProduceMessage(empty) without a receiver did not return within 5s after cancellation, want returned")
	}
}

// TestTopicProduceMessageSendsConcurrently checks storage and each producer's order.
func TestTopicProduceMessageSendsConcurrently(t *testing.T) {
	topic, _, stop := newTestTopic(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	const producers, count = 8, 32
	start := make(chan struct{})
	results := make(chan error, producers)
	var workers sync.WaitGroup
	for producer := 0; producer < producers; producer++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for i := 0; i < count; i++ {
				if err := topic.ProduceMessage(ctx, protocol.NewMessage([]byte{byte(producer)}, []byte{byte(i)})); err != nil {
					results <- fmt.Errorf("ProduceMessage(producer %d, message %d): %w", producer, i, err)
					return
				}
			}
			results <- nil
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("concurrent ProduceMessage() error = %v, want nil", err)
		}
	}
	stop()
	if !waitTopicLoop(t, topic) {
		return
	}
	if got := len(topic.messages); got != producers*count {
		t.Errorf("stored message count after concurrent sends = %d, want %d", got, producers*count)
	}
	next := make([]int, producers)
	wantOffsets := make([][]uint32, producers)
	for offset, message := range topic.messages {
		key, value := message.Key(), message.Value()
		if len(key) != 1 || len(value) != 1 || int(key[0]) >= producers {
			t.Errorf("stored message at offset %d = key %x, value %x, want a producer byte and a sequence byte", offset, key, value)
			continue
		}
		producer := int(key[0])
		if int(value[0]) != next[producer] {
			t.Errorf("stored message at offset %d for producer %d has sequence %d, want %d", offset, producer, value[0], next[producer])
		}
		next[producer]++
		wantOffsets[producer] = append(wantOffsets[producer], uint32(offset))
	}
	for producer, got := range next {
		if got != count {
			t.Errorf("stored count for producer %d = %d, want %d", producer, got, count)
		}
		offsets, err := topic.messageOffsetTrie.Search(string([]byte{byte(producer)}), 0)
		if err != nil || !slices.Equal(offsets, wantOffsets[producer]) {
			t.Errorf("Search(producer %d, 0) = %v, %v, want %v and nil", producer, offsets, err, wantOffsets[producer])
		}
	}
}

// TestTopicProduceMessageRejectsStoppedTopic checks sends after the receiver ends.
func TestTopicProduceMessageRejectsStoppedTopic(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, expireCancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer expireCancel()
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{name: "canceled", ctx: canceled, wantErr: context.Canceled},
		{name: "expired", ctx: expired, wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := NewTopic(tt.ctx)
			if !waitTopicLoop(t, topic) {
				return
			}
			ctx, cancelRequest := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelRequest()
			if err := topic.ProduceMessage(ctx, protocol.NewMessage(nil, nil)); !errors.Is(err, tt.wantErr) {
				t.Errorf("ProduceMessage(empty) after topic %s error = %v, want %v", tt.name, err, tt.wantErr)
			}
		})
	}
}

// TestTopicStorageFailureStopsReceiver checks failure propagation to later producers.
func TestTopicStorageFailureStopsReceiver(t *testing.T) {
	topic, _, _ := newTestTopic(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	topic.mu.Lock()
	topic.messageOffsetTrie = &MessageOffsetTrie{}
	topic.mu.Unlock()
	if err := topic.ProduceMessage(ctx, protocol.NewMessage(nil, nil)); err != nil {
		t.Errorf("ProduceMessage(empty) before index failure error = %v, want nil for the channel handoff", err)
		return
	}
	if !waitTopicLoop(t, topic) {
		return
	}
	if len(topic.messages) != 0 {
		t.Errorf("stored message count after index failure = %d, want 0", len(topic.messages))
	}
	if err := topic.ProduceMessage(t.Context(), protocol.NewMessage(nil, nil)); !errors.Is(err, ErrMessageOffsetTrieUninitialized) {
		t.Errorf("ProduceMessage(empty) after index failure error = %v, want %v", err, ErrMessageOffsetTrieUninitialized)
	}
}

// TestTopicConsumerShutdown checks that each termination path stops workers and removes the subscription.
func TestTopicConsumerShutdown(t *testing.T) {
	tests := []struct {
		name string
		stop func(*Topic, *consumer.Consumer, context.CancelFunc) error
	}{
		{name: "explicit removal", stop: func(topic *Topic, c *consumer.Consumer, _ context.CancelFunc) error {
			return topic.RemoveConsumer(c)
		}},
		{name: "connection closed", stop: func(_ *Topic, c *consumer.Consumer, _ context.CancelFunc) error {
			return c.Connection().Close()
		}},
		{name: "parent canceled", stop: func(_ *Topic, _ *consumer.Consumer, cancel context.CancelFunc) error {
			cancel()
			return nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, ctx, cancel := newTestTopic(t)
			c := connectedTopicConsumer(t, "events/", "current")
			if c == nil {
				return
			}
			if err := topic.upsertConsumer(ctx, c, nil, nil, nil); err != nil {
				t.Errorf("upsertConsumer(consumer-1, events/) error = %v, want nil", err)
				return
			}
			if err := tt.stop(topic, c, cancel); err != nil {
				t.Errorf("%s for consumer-1 error = %v, want nil", tt.name, err)
				return
			}
			if !waitTopicWorkers(t, topic) {
				return
			}
			topic.mu.Lock()
			count := topic.consumerMap.Len()
			topic.mu.Unlock()
			if count != 0 {
				t.Errorf("consumer index after %s has %d entries, want 0", tt.name, count)
			}
			got, err := topic.consumerTrie.Search("events/item")
			if err != nil || len(got) != 0 {
				t.Errorf("Search(events/item) after %s = %v, error = %v, want no subscribers and nil error", tt.name, got, err)
			}
			select {
			case <-c.Connection().Done():
			default:
				t.Errorf("Connection().Done() after %s is open, want closed", tt.name)
			}
		})
	}
}

// TestTopicConsumerWorkersAwaitConfirmation checks heartbeat processing after confirmation.
func TestTopicConsumerWorkersAwaitConfirmation(t *testing.T) {
	topic, ctx, _ := newTestTopic(t)
	c := connectedTopicConsumer(t, "events/", "pending")
	if c == nil {
		return
	}
	confirmed := make(chan struct{})
	if err := topic.upsertConsumer(ctx, c, confirmed, nil, nil); err != nil {
		t.Errorf("upsertConsumer(consumer-1) before confirmation error = %v, want nil", err)
		return
	}
	select {
	case c.Connection().HeartbeatChan <- struct{}{}:
		t.Error("consumer-1 heartbeat before confirmation was consumed, want pending")
	default:
	}
	close(confirmed)
	select {
	case c.Connection().HeartbeatChan <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Error("consumer-1 heartbeat after confirmation was not consumed within 5s, want consumed")
	}
}

// TestTopicPendingConsumerShutdown checks cleanup without confirmation.
func TestTopicPendingConsumerShutdown(t *testing.T) {
	tests := []struct {
		name string
		stop func(*Topic, *consumer.Consumer, context.CancelFunc) error
	}{
		{name: "explicit removal", stop: func(topic *Topic, c *consumer.Consumer, _ context.CancelFunc) error {
			return topic.RemoveConsumer(c)
		}},
		{name: "connection closed", stop: func(_ *Topic, c *consumer.Consumer, _ context.CancelFunc) error {
			return c.Connection().Close()
		}},
		{name: "parent canceled", stop: func(_ *Topic, _ *consumer.Consumer, cancel context.CancelFunc) error {
			cancel()
			return nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, ctx, cancel := newTestTopic(t)
			c := connectedTopicConsumer(t, "events/", "pending")
			if c == nil {
				return
			}
			var calls atomic.Int32
			if err := topic.upsertConsumer(ctx, c, make(chan struct{}), func() { calls.Add(1) }, nil); err != nil {
				t.Errorf("upsertConsumer(consumer-1) before %s error = %v, want nil", tt.name, err)
				return
			}
			if err := tt.stop(topic, c, cancel); err != nil {
				t.Errorf("%s for pending consumer-1 error = %v, want nil", tt.name, err)
				return
			}
			if !waitTopicWorkers(t, topic) {
				return
			}
			if got := calls.Load(); got != 1 {
				t.Errorf("cleanup count after %s before confirmation = %d, want 1", tt.name, got)
			}
			topic.mu.Lock()
			attached := topic.consumerMap.Len()
			topic.mu.Unlock()
			if attached != 0 {
				t.Errorf("attached count after %s before confirmation = %d, want 0", tt.name, attached)
			}
		})
	}
}

// TestTopicConsumerWorkersRunCleanupOnce checks the existing workers' termination paths.
func TestTopicConsumerWorkersRunCleanupOnce(t *testing.T) {
	tests := []struct {
		name string
		stop func(*Topic, *consumer.Consumer, context.CancelFunc) error
	}{
		{name: "explicit removal", stop: func(topic *Topic, c *consumer.Consumer, _ context.CancelFunc) error {
			return topic.RemoveConsumer(c)
		}},
		{name: "connection closed", stop: func(_ *Topic, c *consumer.Consumer, _ context.CancelFunc) error {
			return c.Connection().Close()
		}},
		{name: "parent canceled", stop: func(_ *Topic, _ *consumer.Consumer, cancel context.CancelFunc) error {
			cancel()
			return nil
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, ctx, cancel := newTestTopic(t)
			c := connectedTopicConsumer(t, "events/", "current")
			if c == nil {
				return
			}
			var calls atomic.Int32
			if err := topic.upsertConsumer(ctx, c, nil, func() { calls.Add(1) }, nil); err != nil {
				t.Fatalf("upsertConsumer(consumer-1, cleanup) error = %v, want nil", err)
			}
			if err := tt.stop(topic, c, cancel); err != nil {
				t.Errorf("%s for consumer-1 error = %v, want nil", tt.name, err)
				return
			}
			if !waitTopicWorkers(t, topic) {
				return
			}
			if got := calls.Load(); got != 1 {
				t.Errorf("cleanup count after %s for consumer-1 = %d, want 1", tt.name, got)
			}
			topic.mu.Lock()
			count := topic.consumerMap.Len()
			topic.mu.Unlock()
			if count != 0 {
				t.Errorf("consumer count after %s and cleanup = %d, want 0", tt.name, count)
			}
		})
	}
}

// TestTopicConsumerCleanupDoesNotRunAfterFailedRegistration checks cleanup ownership.
func TestTopicConsumerCleanupDoesNotRunAfterFailedRegistration(t *testing.T) {
	topic, ctx, _ := newTestTopic(t)
	c := connectedTopicConsumer(t, "events/", "current")
	if c == nil {
		return
	}
	topic.consumerMap = &hashmap.HashMap[id.ID, *subscription]{}
	var calls atomic.Int32
	if err := topic.upsertConsumer(ctx, c, nil, func() { calls.Add(1) }, nil); !errors.Is(err, hashmap.ErrUninitialized) {
		t.Errorf("upsertConsumer(consumer-1, cleanup) with an invalid index error = %v, want %v", err, hashmap.ErrUninitialized)
	}
	if !waitTopicWorkers(t, topic) {
		return
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("cleanup count after rejected registration for consumer-1 = %d, want 0", got)
	}
}

// TestTopicConsumerReplacement checks that stale removal cannot detach the replacement connection.
func TestTopicConsumerReplacement(t *testing.T) {
	tests := []struct {
		name               string
		prefix             string
		wantOldPrefixCount int
	}{
		{name: "same prefix", prefix: "events/", wantOldPrefixCount: 1},
		{name: "changed prefix", prefix: "orders/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, ctx, _ := newTestTopic(t)
			previous := connectedTopicConsumer(t, "events/", "previous")
			current := connectedTopicConsumer(t, tt.prefix, "current")
			if previous == nil || current == nil {
				return
			}
			if err := topic.upsertConsumer(ctx, previous, nil, nil, nil); err != nil {
				t.Errorf("upsertConsumer(previous consumer-1, events/) error = %v, want nil", err)
				return
			}
			if err := topic.upsertConsumer(ctx, current, nil, nil, nil); err != nil {
				t.Errorf("upsertConsumer(replacement consumer-1, %q) error = %v, want nil", tt.prefix, err)
				return
			}
			if err := topic.RemoveConsumer(previous); err != nil {
				t.Errorf("RemoveConsumer(previous consumer-1) error = %v, want nil", err)
			}
			topic.mu.Lock()
			stored, exists := topic.consumerMap.Get(current.Info.ID())
			matches := exists && stored.connection == current.Connection() && stored.info == current.Info
			topic.mu.Unlock()
			if !matches {
				t.Errorf("stored consumer-1 after replacing prefix with %q and removing the old connection does not match the replacement, want its metadata and connection", tt.prefix)
			}
			got, err := topic.consumerTrie.Search(tt.prefix + "item")
			if err != nil || len(got) != 1 || got[0] != current.Info.ID() {
				t.Errorf("Search(%q) after replacement = %v, error = %v, want [consumer-1] and nil error", tt.prefix+"item", got, err)
			}
			oldKey := previous.Info.Prefix() + "item"
			got, err = topic.consumerTrie.Search(oldKey)
			if err != nil || len(got) != tt.wantOldPrefixCount {
				t.Errorf("Search(%q) after replacement with prefix %q = %v, error = %v, want %d subscribers and nil error", oldKey, tt.prefix, got, err, tt.wantOldPrefixCount)
			}
			select {
			case <-previous.Connection().Done():
			default:
				t.Errorf("previous consumer-1 Connection().Done() after replacement is open, want closed")
			}
			select {
			case <-current.Connection().Done():
				t.Errorf("replacement consumer-1 Connection().Done() after stale removal is closed, want open")
			default:
			}
		})
	}
}

// TestTopicUpsertRejectsDuplicateConnection checks that a rejected update leaves the workers active.
func TestTopicUpsertRejectsDuplicateConnection(t *testing.T) {
	topic, ctx, _ := newTestTopic(t)
	c := connectedTopicConsumer(t, "events/", "current")
	if c == nil {
		return
	}
	if err := topic.upsertConsumer(ctx, c, nil, nil, nil); err != nil {
		t.Errorf("upsertConsumer(consumer-1, events/) error = %v, want nil", err)
		return
	}
	if err := topic.upsertConsumer(ctx, c, nil, nil, nil); !errors.Is(err, ErrInvalidConsumer) {
		t.Errorf("upsertConsumer(consumer-1, same connection) error = %v, want %v", err, ErrInvalidConsumer)
	}
	select {
	case <-c.Connection().Done():
		t.Errorf("consumer-1 Connection().Done() after rejected duplicate is closed, want open")
	default:
	}
}

// TestTopicUpsertRejectsDisconnectedConsumer checks that valid metadata alone cannot start workers.
func TestTopicUpsertRejectsDisconnectedConsumer(t *testing.T) {
	manager := consumer.NewManager()
	if err := manager.AddConsumer("consumer-1", "events", 0, "", "/unused"); err != nil {
		t.Errorf("AddConsumer(consumer-1, events, 0, empty prefix, /unused) error = %v, want nil", err)
		return
	}
	c, err := manager.GetConsumer("consumer-1")
	if err != nil {
		t.Errorf("GetConsumer(consumer-1) error = %v, want nil", err)
		return
	}
	topic, ctx, _ := newTestTopic(t)
	if err := topic.upsertConsumer(ctx, c, nil, nil, nil); !errors.Is(err, ErrInvalidConsumer) {
		t.Errorf("upsertConsumer(disconnected consumer-1) error = %v, want %v", err, ErrInvalidConsumer)
	}
}

// TestTopicUninitialized checks that both consumer operations reject unusable topic state.
func TestTopicUninitialized(t *testing.T) {
	tests := []struct {
		name  string
		topic *Topic
	}{
		{name: "nil receiver"},
		{name: "zero value", topic: &Topic{}},
		{name: "missing consumer index", topic: &Topic{consumerTrie: NewConsumerTrie()}},
		{name: "missing prefix index", topic: &Topic{consumerMap: hashmap.NewHashMap[id.ID, *subscription]()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.topic.upsertConsumer(context.Background(), nil, nil, nil, nil); !errors.Is(err, ErrUninitialized) {
				t.Errorf("upsertConsumer(nil) on %s error = %v, want %v", tt.name, err, ErrUninitialized)
			}
			if err := tt.topic.RemoveConsumer(nil); !errors.Is(err, ErrUninitialized) {
				t.Errorf("RemoveConsumer(nil) on %s error = %v, want %v", tt.name, err, ErrUninitialized)
			}
		})
	}
}

// TestTopicRejectsInvalidConsumer checks that invalid consumers cannot alter either index.
func TestTopicRejectsInvalidConsumer(t *testing.T) {
	tests := []struct {
		name     string
		consumer *consumer.Consumer
	}{
		{name: "nil consumer"},
		{name: "zero consumer", consumer: &consumer.Consumer{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, ctx, _ := newTestTopic(t)
			if err := topic.upsertConsumer(ctx, tt.consumer, nil, nil, nil); !errors.Is(err, ErrInvalidConsumer) {
				t.Errorf("upsertConsumer(%s) error = %v, want %v", tt.name, err, ErrInvalidConsumer)
			}
			if err := topic.RemoveConsumer(tt.consumer); !errors.Is(err, ErrInvalidConsumer) {
				t.Errorf("RemoveConsumer(%s) error = %v, want %v", tt.name, err, ErrInvalidConsumer)
			}
			if got := topic.consumerMap.Len(); got != 0 {
				t.Errorf("consumer index after rejecting %s has %d entries, want 0", tt.name, got)
			}
			got, err := topic.consumerTrie.Search("")
			if err != nil || len(got) != 0 {
				t.Errorf("Search(empty key) after rejecting %s = %v, error = %v, want no subscribers and nil error", tt.name, got, err)
			}
		})
	}
}

// TestTopicUpsertRejectsStoppedContext checks that canceled and expired requests leave connections open.
func TestTopicUpsertRejectsStoppedContext(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, expireCancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer expireCancel()
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{name: "canceled", ctx: canceled, wantErr: context.Canceled},
		{name: "expired", ctx: expired, wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic, _, _ := newTestTopic(t)
			c := connectedTopicConsumer(t, "events/", "current")
			if c == nil {
				return
			}
			if err := topic.upsertConsumer(tt.ctx, c, nil, nil, nil); !errors.Is(err, tt.wantErr) {
				t.Errorf("upsertConsumer(consumer-1) with %s context error = %v, want %v", tt.name, err, tt.wantErr)
			}
			if !waitTopicWorkers(t, topic) {
				return
			}
			if got := topic.consumerMap.Len(); got != 0 {
				t.Errorf("consumer index after %s request has %d entries, want 0", tt.name, got)
			}
			got, err := topic.consumerTrie.Search("events/item")
			if err != nil || len(got) != 0 {
				t.Errorf("Search(events/item) after %s request = %v, error = %v, want no subscribers and nil error", tt.name, got, err)
			}
			select {
			case <-c.Connection().Done():
				t.Errorf("consumer-1 Connection().Done() after %s request is closed, want open", tt.name)
			default:
			}
		})
	}
}

// TestTopicUpsertRollsBackFailedInsert checks that a failed consumer-index insertion removes the new prefix.
func TestTopicUpsertRollsBackFailedInsert(t *testing.T) {
	topic, ctx, _ := newTestTopic(t)
	c := connectedTopicConsumer(t, "events/", "current")
	if c == nil {
		return
	}
	// An uninitialized map fails insertion after the prefix has been subscribed.
	topic.consumerMap = &hashmap.HashMap[id.ID, *subscription]{}
	if err := topic.upsertConsumer(ctx, c, nil, nil, nil); !errors.Is(err, hashmap.ErrUninitialized) {
		t.Errorf("upsertConsumer(consumer-1, events/) with an uninitialized consumer index error = %v, want %v", err, hashmap.ErrUninitialized)
	}
	got, err := topic.consumerTrie.Search("events/item")
	if err != nil || len(got) != 0 {
		t.Errorf("Search(events/item) after failed insertion = %v, error = %v, want no subscribers and nil error", got, err)
	}
	select {
	case <-c.Connection().Done():
		t.Errorf("consumer-1 Connection().Done() after failed insertion is closed, want open")
	default:
	}
}

// TestTopicUpsertRollsBackFailedPrefixChange checks that failed prefix removal preserves the previous subscription.
func TestTopicUpsertRollsBackFailedPrefixChange(t *testing.T) {
	topic, ctx, _ := newTestTopic(t)
	previous := connectedTopicConsumer(t, "events/", "previous")
	current := connectedTopicConsumer(t, "orders/", "current")
	if previous == nil || current == nil {
		return
	}
	if err := topic.upsertConsumer(ctx, previous, nil, nil, nil); err != nil {
		t.Errorf("upsertConsumer(previous consumer-1, events/) error = %v, want nil", err)
		return
	}
	// Remove the old prefix so moving the subscription fails after adding the new one.
	if err := topic.consumerTrie.Unsubscribe("events/", previous.Info.ID()); err != nil {
		t.Errorf("Unsubscribe(events/, consumer-1) during setup error = %v, want nil", err)
		return
	}
	if err := topic.upsertConsumer(ctx, current, nil, nil, nil); !errors.Is(err, trie.ErrPrefixNotFound) {
		t.Errorf("upsertConsumer(replacement consumer-1, orders/) without its old prefix error = %v, want %v", err, trie.ErrPrefixNotFound)
	}
	topic.mu.Lock()
	stored, exists := topic.consumerMap.Get(previous.Info.ID())
	matches := exists && stored.info == previous.Info && stored.connection == previous.Connection()
	topic.mu.Unlock()
	if !matches {
		t.Errorf("stored consumer-1 after failed prefix change does not match the previous subscription, want its original metadata and connection")
	}
	got, err := topic.consumerTrie.Search("orders/item")
	if err != nil || len(got) != 0 {
		t.Errorf("Search(orders/item) after failed prefix change = %v, error = %v, want no subscribers and nil error", got, err)
	}
	select {
	case <-previous.Connection().Done():
		t.Errorf("previous consumer-1 Connection().Done() after failed prefix change is closed, want open")
	default:
	}
}

// TestTopicRemoveKeepsConnectionOnIndexFailure checks that a failed prefix removal leaves the connection active.
func TestTopicRemoveKeepsConnectionOnIndexFailure(t *testing.T) {
	topic, ctx, _ := newTestTopic(t)
	c := connectedTopicConsumer(t, "events/", "current")
	if c == nil {
		return
	}
	if err := topic.upsertConsumer(ctx, c, nil, nil, nil); err != nil {
		t.Errorf("upsertConsumer(consumer-1, events/) error = %v, want nil", err)
		return
	}
	if err := topic.consumerTrie.Unsubscribe("events/", c.Info.ID()); err != nil {
		t.Errorf("Unsubscribe(events/, consumer-1) during setup error = %v, want nil", err)
		return
	}
	if err := topic.RemoveConsumer(c); !errors.Is(err, trie.ErrPrefixNotFound) {
		t.Errorf("RemoveConsumer(consumer-1) without its prefix error = %v, want %v", err, trie.ErrPrefixNotFound)
	}
	topic.mu.Lock()
	stored, exists := topic.consumerMap.Get(c.Info.ID())
	matches := exists && stored.info == c.Info && stored.connection == c.Connection()
	topic.mu.Unlock()
	if !matches {
		t.Errorf("stored consumer-1 after failed removal does not match the original subscription, want its metadata and connection")
	}
	select {
	case <-c.Connection().Done():
		t.Errorf("consumer-1 Connection().Done() after failed removal is closed, want open")
	default:
	}
}
