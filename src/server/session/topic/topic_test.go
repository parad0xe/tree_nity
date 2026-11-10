package topic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/hashmap"
	"tree_nity/src/server/utils/trie"
)

func waitTopicWorkers(t *testing.T, topic *Topic) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		topic.waitGroup.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Errorf("topic workers after cancellation did not stop within 5s, want all workers stopped")
	}
}

func newTestTopic(t *testing.T) (*Topic, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	topic := NewTopic(ctx)
	t.Cleanup(func() {
		cancel()
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
			if err := topic.UpsertConsumer(ctx, c); err != nil {
				t.Errorf("UpsertConsumer(consumer-1, events/) error = %v, want nil", err)
				return
			}
			if err := tt.stop(topic, c, cancel); err != nil {
				t.Errorf("%s for consumer-1 error = %v, want nil", tt.name, err)
				return
			}
			waitTopicWorkers(t, topic)
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

// TestTopicConsumerReplacement checks that stale removal cannot detach the replacement connection.
func TestTopicConsumerReplacement(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
	}{
		{name: "same prefix", prefix: "events/"},
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
			if err := topic.UpsertConsumer(ctx, previous); err != nil {
				t.Errorf("UpsertConsumer(previous consumer-1, events/) error = %v, want nil", err)
				return
			}
			if err := topic.UpsertConsumer(ctx, current); err != nil {
				t.Errorf("UpsertConsumer(replacement consumer-1, %q) error = %v, want nil", tt.prefix, err)
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
	if err := topic.UpsertConsumer(ctx, c); err != nil {
		t.Errorf("UpsertConsumer(consumer-1, events/) error = %v, want nil", err)
		return
	}
	if err := topic.UpsertConsumer(ctx, c); !errors.Is(err, ErrInvalidConsumer) {
		t.Errorf("UpsertConsumer(consumer-1, same connection) error = %v, want %v", err, ErrInvalidConsumer)
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
	if err := topic.UpsertConsumer(ctx, c); !errors.Is(err, ErrInvalidConsumer) {
		t.Errorf("UpsertConsumer(disconnected consumer-1) error = %v, want %v", err, ErrInvalidConsumer)
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
			if err := tt.topic.UpsertConsumer(context.Background(), nil); !errors.Is(err, ErrUninitialized) {
				t.Errorf("UpsertConsumer(nil) on %s error = %v, want %v", tt.name, err, ErrUninitialized)
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
			if err := topic.UpsertConsumer(ctx, tt.consumer); !errors.Is(err, ErrInvalidConsumer) {
				t.Errorf("UpsertConsumer(%s) error = %v, want %v", tt.name, err, ErrInvalidConsumer)
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
			if err := topic.UpsertConsumer(tt.ctx, c); !errors.Is(err, tt.wantErr) {
				t.Errorf("UpsertConsumer(consumer-1) with %s context error = %v, want %v", tt.name, err, tt.wantErr)
			}
			waitTopicWorkers(t, topic)
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
	if err := topic.UpsertConsumer(ctx, c); !errors.Is(err, hashmap.ErrUninitialized) {
		t.Errorf("UpsertConsumer(consumer-1, events/) with an uninitialized consumer index error = %v, want %v", err, hashmap.ErrUninitialized)
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
	if err := topic.UpsertConsumer(ctx, previous); err != nil {
		t.Errorf("UpsertConsumer(previous consumer-1, events/) error = %v, want nil", err)
		return
	}
	// Remove the old prefix so moving the subscription fails after adding the new one.
	if err := topic.consumerTrie.Unsubscribe("events/", previous.Info.ID()); err != nil {
		t.Errorf("Unsubscribe(events/, consumer-1) during setup error = %v, want nil", err)
		return
	}
	if err := topic.UpsertConsumer(ctx, current); !errors.Is(err, trie.ErrPrefixNotFound) {
		t.Errorf("UpsertConsumer(replacement consumer-1, orders/) without its old prefix error = %v, want %v", err, trie.ErrPrefixNotFound)
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
	if err := topic.UpsertConsumer(ctx, c); err != nil {
		t.Errorf("UpsertConsumer(consumer-1, events/) error = %v, want nil", err)
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
