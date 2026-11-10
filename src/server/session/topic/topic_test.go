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
