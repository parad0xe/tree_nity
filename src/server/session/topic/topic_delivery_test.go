package topic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
	"tree_nity/src/server/session/consumer"
)

func deliveryInbox(t *testing.T, suffix string) *ipc.Inbound {
	t.Helper()
	path := fmt.Sprintf("/delivery.%d.%s.%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "."), suffix)
	inbound, err := ipc.NewInbound(path, protocol.Decode)
	if err != nil {
		t.Fatalf("NewInbound(%q) error = %v, want nil", path, err)
	}
	t.Cleanup(func() {
		if err := inbound.Drop(); err != nil {
			t.Errorf("Drop(%q) error = %v, want nil", path, err)
		}
	})
	return inbound
}

func subscribeForDelivery(t *testing.T, manager *Manager, lifetime context.Context, inbound *ipc.Inbound, name, prefix string, offset *uint32) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(lifetime)
	t.Cleanup(cancel)
	payload := *protocol.NewTopicSubscriptionPayload(name, "events", prefix, offset)
	if err := manager.NewSubscription(topicManagerContext(t), ctx, lifecycle.GracefulShutdown{}, *protocol.NewClientMetadata(inbound.IPCPath()), payload); err != nil {
		t.Fatalf("NewSubscription(%q, events, %q, %v) error = %v, want nil", name, prefix, offset, err)
	}
	receiveTopicManagerReply(t, topicManagerContext(t), inbound, protocol.TopicSubscriptionReply)
	return cancel
}

func receiveDelivery(t *testing.T, inbound *ipc.Inbound, offset uint32, want protocol.Message) {
	t.Helper()
	data := receiveTopicManagerReply(t, topicManagerContext(t), inbound, protocol.MessageDelivery)
	payload, ok := data.Payload().(*protocol.MessageDeliveryPayload)
	if !ok {
		t.Fatalf("delivery at offset %d payload = %T, want *protocol.MessageDeliveryPayload", offset, data.Payload())
	}
	got := payload.Message()
	if got.Offset() != offset || !bytes.Equal(got.Key(), want.Key()) || !bytes.Equal(got.Value(), want.Value()) {
		t.Errorf("delivery = offset %d, key %x, value %x, want offset %d, key %x, value %x", got.Offset(), got.Key(), got.Value(), offset, want.Key(), want.Value())
	}
}

func acknowledgeDelivery(t *testing.T, manager *Manager, inbound *ipc.Inbound, name string, offset uint32) {
	t.Helper()
	err := manager.Acknowledgement(topicManagerContext(t), *protocol.NewClientMetadata(inbound.IPCPath()), *protocol.NewAcknowledgementPayload(name, offset))
	if err != nil {
		t.Fatalf("Acknowledgement(%q, %d) error = %v, want nil", name, offset, err)
	}
}

func checkSavedOffset(t *testing.T, manager *Manager, name string, offset uint32) {
	t.Helper()
	c, err := manager.consumerManager.GetConsumer(id.ID(name))
	if err != nil {
		t.Errorf("GetConsumer(%q) error = %v, want nil", name, err)
	} else if got := c.Info.Offset(); got != offset {
		t.Errorf("saved offset for %q = %d, want %d", name, got, offset)
	}
}

func expectNoDelivery(t *testing.T, inbound *ipc.Inbound) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	data, err := inbound.Recv(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Recv(%q) while no delivery is allowed = %+v, %v, want %v", inbound.IPCPath(), data, err, context.DeadlineExceeded)
	}
}

func deliveryMessage(index int) protocol.Message {
	key := "user/"
	if index%2 != 0 {
		key = "other/"
	}
	return protocol.NewMessage(append([]byte(key), byte(index), 0xff), []byte{0, 0xff, byte(index)})
}

// TestDeliveryStreamsWhileProducing checks batch ownership during concurrent production and ACKs.
func TestDeliveryStreamsWhileProducing(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	inbound := deliveryInbox(t, "consumer")
	subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "", nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	result := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("concurrent producer did not stop within 5s, want stopped")
		}
	})
	const count = 512
	go func() {
		defer close(finished)
		for index := 0; index < count; index++ {
			if err := target.ProduceMessage(ctx, deliveryMessage(index)); err != nil {
				result <- err
				return
			}
		}
		result <- nil
	}()
	for index := 0; index < count; index++ {
		receiveDelivery(t, inbound, uint32(index), deliveryMessage(index))
		acknowledgeDelivery(t, manager, inbound, "subscriber", uint32(index+1))
	}
	select {
	case err := <-result:
		if err != nil {
			t.Errorf("ProduceMessage(%d concurrent messages) error = %v, want nil", count, err)
		}
	case <-ctx.Done():
		t.Errorf("ProduceMessage(%d concurrent messages) did not finish: %v, want nil", count, ctx.Err())
	}
	checkSavedOffset(t, manager, "subscriber", count)
	expectNoDelivery(t, inbound)
}

// TestDeliveryPreservesPendingBatches checks new arrivals while a replay batch waits for ACKs.
func TestDeliveryPreservesPendingBatches(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	ctx := topicManagerContext(t)
	const history = 5
	for index := 0; index < history; index++ {
		if err := target.ProduceMessage(ctx, deliveryMessage(index)); err != nil {
			t.Fatalf("ProduceMessage(history %d) error = %v, want nil", index, err)
		}
	}
	inbound := deliveryInbox(t, "consumer")
	subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "", nil)
	receiveDelivery(t, inbound, 0, deliveryMessage(0))
	const burst = 128
	for index := history; index < history+burst; index++ {
		if err := target.ProduceMessage(ctx, deliveryMessage(index)); err != nil {
			t.Fatalf("ProduceMessage(first burst %d) error = %v, want nil", index, err)
		}
	}
	acknowledgeDelivery(t, manager, inbound, "subscriber", 1)
	receiveDelivery(t, inbound, 1, deliveryMessage(1))
	for index := history + burst; index < history+2*burst; index++ {
		if err := target.ProduceMessage(ctx, deliveryMessage(index)); err != nil {
			t.Fatalf("ProduceMessage(second burst %d) error = %v, want nil", index, err)
		}
	}
	expectNoDelivery(t, inbound)
	checkSavedOffset(t, manager, "subscriber", 1)
	acknowledgeDelivery(t, manager, inbound, "subscriber", 2)
	for index := 2; index < history+2*burst; index++ {
		receiveDelivery(t, inbound, uint32(index), deliveryMessage(index))
		acknowledgeDelivery(t, manager, inbound, "subscriber", uint32(index+1))
	}
	checkSavedOffset(t, manager, "subscriber", history+2*burst)
	expectNoDelivery(t, inbound)
}

// TestDeliveryWakesAfterIdle checks notifications after catch-up, including nonmatching messages.
func TestDeliveryWakesAfterIdle(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	inbound := deliveryInbox(t, "consumer")
	subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "user/", nil)
	expectNoDelivery(t, inbound)
	for index := 0; index < 3; index++ {
		if err := target.ProduceMessage(topicManagerContext(t), deliveryMessage(index)); err != nil {
			t.Fatalf("ProduceMessage(%d) after catch-up error = %v, want nil", index, err)
		}
		if index%2 == 0 {
			receiveDelivery(t, inbound, uint32(index), deliveryMessage(index))
			acknowledgeDelivery(t, manager, inbound, "subscriber", uint32(index+1))
		}
		expectNoDelivery(t, inbound)
	}
	checkSavedOffset(t, manager, "subscriber", 3)
}

// TestDeliveryReplaysThenFollowsLive checks filtering and coalesced wakeups across replay.
func TestDeliveryReplaysThenFollowsLive(t *testing.T) {
	tests := []struct {
		name    string
		history int
		start   uint32
		prefix  string
	}{
		{name: "empty history"},
		{name: "one stored message", history: 1},
		{name: "prefix", history: 8, prefix: "user/"},
		{name: "custom offset", history: 8, start: 3, prefix: "user/"},
		{name: "future offset", history: 2, start: 10},
		{name: "no matching keys", history: 8, prefix: "missing/"},
		{name: "large history", history: 256},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			target, lifetime, _ := subscriptionTarget(t, manager, "events")
			ctx := topicManagerContext(t)
			for index := 0; index < tt.history; index++ {
				if err := target.ProduceMessage(ctx, deliveryMessage(index)); err != nil {
					t.Fatalf("ProduceMessage(history %d) error = %v, want nil", index, err)
				}
			}
			inbound := deliveryInbox(t, "consumer")
			subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", tt.prefix, &tt.start)
			const live = 32
			for index := tt.history; index < tt.history+live; index++ {
				if err := target.ProduceMessage(ctx, deliveryMessage(index)); err != nil {
					t.Fatalf("ProduceMessage(live %d) while replaying error = %v, want nil", index, err)
				}
			}
			first := true
			for index := int(tt.start); index < tt.history+live; index++ {
				message := deliveryMessage(index)
				if !bytes.HasPrefix(message.Key(), []byte(tt.prefix)) {
					continue
				}
				receiveDelivery(t, inbound, uint32(index), message)
				if first {
					checkSavedOffset(t, manager, "subscriber", tt.start)
					expectNoDelivery(t, inbound)
					first = false
				}
				acknowledgeDelivery(t, manager, inbound, "subscriber", uint32(index+1))
				checkSavedOffset(t, manager, "subscriber", uint32(index+1))
			}
			expectNoDelivery(t, inbound)
			if first {
				checkSavedOffset(t, manager, "subscriber", tt.start)
			}
		})
	}
}

// TestDeliveryRejectsIncorrectAcknowledgements checks that only the pending delivery advances.
func TestDeliveryRejectsIncorrectAcknowledgements(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	ctx := topicManagerContext(t)
	for index := 0; index < 2; index++ {
		if err := target.ProduceMessage(ctx, deliveryMessage(index)); err != nil {
			t.Fatalf("ProduceMessage(%d) during setup error = %v, want nil", index, err)
		}
	}
	inbound := deliveryInbox(t, "consumer")
	subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "", nil)
	receiveDelivery(t, inbound, 0, deliveryMessage(0))
	tests := []struct {
		name   string
		offset uint32
	}{
		{name: "received offset", offset: 0},
		{name: "future offset", offset: 2},
		{name: "maximum offset", offset: ^uint32(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := manager.Acknowledgement(ctx, *protocol.NewClientMetadata(inbound.IPCPath()), *protocol.NewAcknowledgementPayload("subscriber", tt.offset))
			if !errors.Is(err, protocol.ErrInvalidPayload) {
				t.Errorf("Acknowledgement(subscriber, %d) error = %v, want %v", tt.offset, err, protocol.ErrInvalidPayload)
			}
			checkSavedOffset(t, manager, "subscriber", 0)
		})
	}
	expectNoDelivery(t, inbound)
	acknowledgeDelivery(t, manager, inbound, "subscriber", 1)
	receiveDelivery(t, inbound, 1, deliveryMessage(1))
	err := manager.Acknowledgement(ctx, *protocol.NewClientMetadata(inbound.IPCPath()), *protocol.NewAcknowledgementPayload("subscriber", 1))
	if !errors.Is(err, protocol.ErrInvalidPayload) {
		t.Errorf("duplicate Acknowledgement(subscriber, 1) error = %v, want %v", err, protocol.ErrInvalidPayload)
	}
	checkSavedOffset(t, manager, "subscriber", 1)
	acknowledgeDelivery(t, manager, inbound, "subscriber", 2)
	checkSavedOffset(t, manager, "subscriber", 2)
	expectNoDelivery(t, inbound)
}

// TestDeliveryResumesUnacknowledgedMessages checks committed progress and stale connections.
func TestDeliveryResumesUnacknowledgedMessages(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	ctx := topicManagerContext(t)
	for index := 0; index < 3; index++ {
		if err := target.ProduceMessage(ctx, deliveryMessage(index)); err != nil {
			t.Fatalf("ProduceMessage(%d) during setup error = %v, want nil", index, err)
		}
	}
	first := deliveryInbox(t, "first")
	stopFirst := subscribeForDelivery(t, manager, lifetime, first, "subscriber", "", nil)
	receiveDelivery(t, first, 0, deliveryMessage(0))
	acknowledgeDelivery(t, manager, first, "subscriber", 1)
	receiveDelivery(t, first, 1, deliveryMessage(1))
	stopFirst()
	if !waitTopicWorkers(t, target) {
		return
	}
	checkSavedOffset(t, manager, "subscriber", 1)
	second := deliveryInbox(t, "second")
	subscribeForDelivery(t, manager, lifetime, second, "subscriber", "", nil)
	receiveDelivery(t, second, 1, deliveryMessage(1))
	err := manager.Acknowledgement(ctx, *protocol.NewClientMetadata(first.IPCPath()), *protocol.NewAcknowledgementPayload("subscriber", 2))
	if !errors.Is(err, consumer.ErrNotFound) {
		t.Errorf("Acknowledgement(subscriber, 2) from replaced IPC error = %v, want %v", err, consumer.ErrNotFound)
	}
	checkSavedOffset(t, manager, "subscriber", 1)
	acknowledgeDelivery(t, manager, second, "subscriber", 2)
	receiveDelivery(t, second, 2, deliveryMessage(2))
	acknowledgeDelivery(t, manager, second, "subscriber", 3)
	checkSavedOffset(t, manager, "subscriber", 3)
	expectNoDelivery(t, second)
}

// TestDeliverySlowConsumerDoesNotBlockOthers checks independent delivery and storage.
func TestDeliverySlowConsumerDoesNotBlockOthers(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	ctx := topicManagerContext(t)
	slow := deliveryInbox(t, "slow")
	fast := deliveryInbox(t, "fast")
	subscribeForDelivery(t, manager, lifetime, slow, "slow", "", nil)
	subscribeForDelivery(t, manager, lifetime, fast, "fast", "", nil)
	const count = 64
	for index := 0; index < count; index++ {
		if err := target.ProduceMessage(ctx, deliveryMessage(index)); err != nil {
			t.Fatalf("ProduceMessage(%d) with an unacknowledging consumer error = %v, want nil", index, err)
		}
	}
	receiveDelivery(t, slow, 0, deliveryMessage(0))
	for index := 0; index < count; index++ {
		receiveDelivery(t, fast, uint32(index), deliveryMessage(index))
		acknowledgeDelivery(t, manager, fast, "fast", uint32(index+1))
	}
	checkSavedOffset(t, manager, "slow", 0)
	checkSavedOffset(t, manager, "fast", count)
	expectNoDelivery(t, slow)
}

// deliverySendContext signals when the worker begins its timed IPC send.
type deliverySendContext struct {
	context.Context
	sending chan struct{}
	once    sync.Once
}

func (c *deliverySendContext) Deadline() (time.Time, bool) {
	c.once.Do(func() { close(c.sending) })
	return c.Context.Deadline()
}

// TestDeliveryCancellationPreservesOffset checks idle, ACK, and full-queue waits.
func TestDeliveryCancellationPreservesOffset(t *testing.T) {
	tests := []struct {
		name      string
		message   bool
		fullQueue bool
		stopTopic bool
	}{
		{name: "idle subscription"},
		{name: "waiting for ACK", message: true},
		{name: "full consumer queue", message: true, fullQueue: true},
		{name: "topic stops while waiting for ACK", message: true, stopTopic: true},
		{name: "topic stops with full consumer queue", message: true, fullQueue: true, stopTopic: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			target, lifetime, stopTopic := subscriptionTarget(t, manager, "events")
			observed := &deliverySendContext{Context: lifetime, sending: make(chan struct{})}
			inbound := deliveryInbox(t, "consumer")
			stopConsumer := subscribeForDelivery(t, manager, observed, inbound, "subscriber", "", nil)
			if tt.fullQueue {
				fillSubscriptionInbox(t, inbound)
			}
			if tt.message {
				if err := target.ProduceMessage(topicManagerContext(t), deliveryMessage(0)); err != nil {
					t.Fatalf("ProduceMessage(0) during setup error = %v, want nil", err)
				}
				if tt.fullQueue {
					select {
					case <-observed.sending:
					case <-time.After(5 * time.Second):
						t.Fatal("delivery into a full queue did not reach IPC Send within 5s, want a waiting send")
					}
				} else {
					receiveDelivery(t, inbound, 0, deliveryMessage(0))
				}
			}
			if tt.stopTopic {
				stopTopic()
				if !waitTopicLoop(t, target) {
					return
				}
			} else {
				stopConsumer()
			}
			if !waitTopicWorkers(t, target) {
				return
			}
			checkSavedOffset(t, manager, "subscriber", 0)
			target.mu.Lock()
			attached := target.consumerMap.Len()
			target.mu.Unlock()
			if attached != 0 {
				t.Errorf("attached count after cancellation during %s = %d, want 0", tt.name, attached)
			}
		})
	}
}
