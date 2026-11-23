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
	"testing"
	"testing/synctest"
	"time"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
	"tree_nity/src/server/response"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/utils/hashmap"
)

// topicManagerContext bounds IPC operations and cancels them when the test ends.
func topicManagerContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// subscriptionReplyContext observes the start of reply transmission.
type subscriptionReplyContext struct {
	context.Context
	beforeSend func()
}

func (c subscriptionReplyContext) Deadline() (time.Time, bool) {
	c.beforeSend()
	return c.Context.Deadline()
}

// newTopicManagerInbox creates a client queue that is removed after the test.
func newTopicManagerInbox(t *testing.T) *ipc.Inbound {
	t.Helper()
	ipcPath := fmt.Sprintf("/topic-manager.%d.%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "."))
	inbound, err := ipc.NewInbound(ipcPath, protocol.Decode)
	if err != nil {
		t.Fatalf("NewInbound(%q) error = %v, want nil", ipcPath, err)
	}
	t.Cleanup(func() {
		if err := inbound.Drop(); err != nil {
			t.Errorf("Drop(%q) error = %v, want nil", ipcPath, err)
		}
	})
	return inbound
}

// receiveTopicManagerReply checks the server envelope before returning the response.
func receiveTopicManagerReply(t *testing.T, ctx context.Context, inbound *ipc.Inbound, opcode protocol.Opcode) protocol.Data {
	t.Helper()
	data, err := inbound.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv(%q) error = %v, want nil", inbound.IPCPath(), err)
	}
	if data.Emitter() != protocol.Server {
		t.Errorf("Recv(%q) emitter = %v, want %v", inbound.IPCPath(), data.Emitter(), protocol.Server)
	}
	if _, ok := data.Metadata().(*protocol.ServerMetadata); !ok {
		t.Errorf("Recv(%q) metadata = %T, want *protocol.ServerMetadata", inbound.IPCPath(), data.Metadata())
	}
	if data.Opcode() != opcode {
		t.Errorf("Recv(%q) opcode = %v, want %v", inbound.IPCPath(), data.Opcode(), opcode)
	}
	return data
}

func subscriptionTarget(t *testing.T, manager *Manager, name string) (*Topic, context.Context, context.CancelFunc) {
	t.Helper()
	target, ctx, cancel := newTestTopic(t)
	if err := manager.topicMap.Insert(id.ID(name), target); err != nil {
		t.Fatalf("Insert(%q) during setup error = %v, want nil", name, err)
	}
	return target, ctx, cancel
}

func subscriptionSnapshot(t *testing.T, target *Topic, name id.ID) subscription {
	t.Helper()
	target.mu.Lock()
	defer target.mu.Unlock()
	c, exists := target.consumerMap.Get(name)
	if !exists || c == nil {
		t.Fatalf("subscription %q = %+v, exists %t, want a registered subscription", name, c, exists)
	}
	return *c
}

func expirationInbox(t *testing.T, suffix string) *ipc.Inbound {
	t.Helper()
	path := fmt.Sprintf("/expiry.%d.%s.%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "."), suffix)
	inbound, err := ipc.NewInbound(path, protocol.Decode)
	if err != nil {
		t.Fatalf("NewInbound(%q) error = %v, want nil", path, err)
	}
	t.Cleanup(func() {
		// The expiration tests intentionally let the server unlink this queue.
		if err := inbound.Drop(); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("Drop(%q) after expiration error = %v, want nil or %v", path, err, os.ErrNotExist)
		}
	})
	return inbound
}

func subscribedConnection(t *testing.T, target *Topic, name id.ID) *consumer.Connection {
	t.Helper()
	target.mu.Lock()
	defer target.mu.Unlock()
	c, exists := target.consumerMap.Get(name)
	if !exists || c.connection == nil {
		t.Fatalf("subscription %q = %+v, exists %t, want an active connection", name, c, exists)
	}
	return c.connection
}

func subscriptionDeadline(t *testing.T, target *Topic, name id.ID) time.Time {
	t.Helper()
	target.mu.Lock()
	defer target.mu.Unlock()
	c, exists := target.consumerMap.Get(name)
	if !exists || c.liveness == nil {
		t.Fatalf("subscription %q = %+v, exists %t, want liveness state", name, c, exists)
	}
	return c.liveness.deadline
}

// TestConsumerExpirationReleasesName checks queue removal and resumption at the saved offset.
func TestConsumerExpirationReleasesName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		manager := NewManager()
		target, lifetime, _ := subscriptionTarget(t, manager, "events")
		inbound := expirationInbox(t, "consumer")
		subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "", nil)
		if err := target.storeMessage(deliveryMessage(0)); err != nil {
			t.Fatalf("storeMessage(0) error = %v, want nil", err)
		}
		synctest.Wait()
		receiveDelivery(t, inbound, 0, deliveryMessage(0))
		acknowledgeDelivery(t, manager, inbound, "subscriber", 1)
		synctest.Wait()
		connection := subscribedConnection(t, target, "subscriber")

		// Timers advance the virtual clock; Wait synchronizes worker completion.
		<-time.After(consumerTimeout)
		synctest.Wait()
		if !waitTopicWorkers(t, target) {
			return
		}
		select {
		case <-connection.Done():
		default:
			t.Errorf("Done(subscriber) after %v of inactivity is open, want closed", consumerTimeout)
		}
		target.mu.Lock()
		count := target.consumerMap.Len()
		matches, searchErr := target.consumerTrie.Search("user/item")
		target.mu.Unlock()
		if count != 0 || searchErr != nil || len(matches) != 0 {
			t.Errorf("subscription indexes after expiration = %d, %v, %v, want 0, empty matches and nil", count, matches, searchErr)
		}
		sender, err := mqueue.NewSender(inbound.IPCPath())
		if err == nil {
			if closeErr := sender.Close(); closeErr != nil {
				t.Errorf("Close(%q) for unexpected sender error = %v, want nil", inbound.IPCPath(), closeErr)
			}
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("NewSender(%q) after expiration error = %v, want %v", inbound.IPCPath(), err, os.ErrNotExist)
		}
		checkSavedOffset(t, manager, "subscriber", 1)
		if err := target.storeMessage(deliveryMessage(1)); err != nil {
			t.Fatalf("storeMessage(1) after expiration error = %v, want nil", err)
		}
		if err := connection.Send(t.Context(), protocol.Data{}); !errors.Is(err, context.Canceled) {
			t.Errorf("Send(subscriber) after expiration error = %v, want %v", err, context.Canceled)
		}
		replacement := expirationInbox(t, "consumer")
		subscribeForDelivery(t, manager, lifetime, replacement, "subscriber", "", nil)
		synctest.Wait()
		receiveDelivery(t, replacement, 1, deliveryMessage(1))
		acknowledgeDelivery(t, manager, replacement, "subscriber", 2)
		checkSavedOffset(t, manager, "subscriber", 2)
	})
}

// TestConsumerNotificationsRenewDeadline checks both activity sources across the original deadline.
func TestConsumerNotificationsRenewDeadline(t *testing.T) {
	tests := []struct {
		name   string
		notify func(*Manager, context.Context, protocol.ClientMetadata) error
	}{
		{name: "heartbeat", notify: func(m *Manager, ctx context.Context, metadata protocol.ClientMetadata) error {
			return m.ConsumerHeartbeat(ctx, metadata, *protocol.NewConsumerHeartbeatPayload("subscriber"))
		}},
		{name: "acknowledgement", notify: func(m *Manager, ctx context.Context, metadata protocol.ClientMetadata) error {
			return m.Acknowledgement(ctx, metadata, *protocol.NewAcknowledgementPayload("subscriber", 1))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				manager := NewManager()
				target, lifetime, _ := subscriptionTarget(t, manager, "events")
				inbound := expirationInbox(t, "consumer")
				subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "", nil)
				if err := target.storeMessage(deliveryMessage(0)); err != nil {
					t.Fatalf("storeMessage(0) error = %v, want nil", err)
				}
				synctest.Wait()
				receiveDelivery(t, inbound, 0, deliveryMessage(0))
				connection := subscribedConnection(t, target, "subscriber")
				<-time.After(20 * time.Second)
				metadata := *protocol.NewClientMetadata(inbound.IPCPath())
				if err := tt.notify(manager, t.Context(), metadata); err != nil {
					t.Errorf("%s(subscriber) at 20s error = %v, want nil", tt.name, err)
					return
				}
				synctest.Wait()
				want := time.Now().Add(consumerTimeout)
				if got := subscriptionDeadline(t, target, "subscriber"); !got.Equal(want) {
					t.Errorf("deadline after %s at 20s = %v, want %v", tt.name, got, want)
				}
				if err := response.SendToClient(t.Context(), metadata, protocol.ServerShutdown, protocol.NewServerShutdownPayload()); err != nil {
					t.Fatalf("SendToClient(marker) after %s error = %v, want nil", tt.name, err)
				}
				receiveTopicManagerReply(t, t.Context(), inbound, protocol.ServerShutdown)
				<-time.After(11 * time.Second)
				synctest.Wait()
				select {
				case <-connection.Done():
					t.Errorf("Done(subscriber) at 31s after %s at 20s is closed, want open until 50s", tt.name)
				default:
				}
				<-time.After(19 * time.Second)
				synctest.Wait()
				select {
				case <-connection.Done():
				default:
					t.Errorf("Done(subscriber) at 50s after %s at 20s is open, want closed", tt.name)
				}
			})
		})
	}
}

// TestManagerConsumerHeartbeatRejectsMissingConnections checks one-way lookup failures.
func TestManagerConsumerHeartbeatRejectsMissingConnections(t *testing.T) {
	tests := []struct {
		name     string
		existing bool
		topic    bool
	}{
		{name: "unknown consumer"},
		{name: "missing topic", existing: true},
		{name: "disconnected consumer", existing: true, topic: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			if tt.topic {
				subscriptionTarget(t, manager, "events")
			}
			if tt.existing {
				if err := manager.consumerManager.AddConsumer("subscriber", "events", 17, "", "/subscriber"); err != nil {
					t.Fatalf("AddConsumer(subscriber) during setup error = %v, want nil", err)
				}
			}
			err := manager.ConsumerHeartbeat(t.Context(), *protocol.NewClientMetadata("/subscriber"), *protocol.NewConsumerHeartbeatPayload("subscriber"))
			if !errors.Is(err, consumer.ErrNotFound) {
				t.Errorf("ConsumerHeartbeat(subscriber) with %s error = %v, want %v", tt.name, err, consumer.ErrNotFound)
			}
		})
	}
}

// TestConsumerHeartbeatRejectsInvalidSession checks that rejected notifications preserve the deadline.
func TestConsumerHeartbeatRejectsInvalidSession(t *testing.T) {
	tests := []struct {
		name     string
		wrongIPC bool
		canceled bool
		wantErr  error
	}{
		{name: "wrong IPC", wrongIPC: true, wantErr: consumer.ErrNotFound},
		{name: "canceled context", canceled: true, wantErr: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				manager := NewManager()
				target, lifetime, _ := subscriptionTarget(t, manager, "events")
				inbound := expirationInbox(t, "consumer")
				subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "", nil)
				synctest.Wait()
				before := subscriptionDeadline(t, target, "subscriber")
				<-time.After(time.Second)
				path := inbound.IPCPath()
				if tt.wrongIPC {
					path += ".stale"
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tt.canceled {
					cancel()
				}
				err := manager.ConsumerHeartbeat(ctx, *protocol.NewClientMetadata(path), *protocol.NewConsumerHeartbeatPayload("subscriber"))
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("ConsumerHeartbeat(subscriber) with %s error = %v, want %v", tt.name, err, tt.wantErr)
				}
				if got := subscriptionDeadline(t, target, "subscriber"); !got.Equal(before) {
					t.Errorf("deadline after rejected heartbeat with %s = %v, want %v", tt.name, got, before)
				}
			})
		})
	}
}

// TestInvalidAcknowledgementPreservesDeadline checks that incorrect offsets are not activity.
func TestInvalidAcknowledgementPreservesDeadline(t *testing.T) {
	for _, offset := range []uint32{0, 2, ^uint32(0)} {
		t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				manager := NewManager()
				target, lifetime, _ := subscriptionTarget(t, manager, "events")
				inbound := expirationInbox(t, "consumer")
				subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "", nil)
				if err := target.storeMessage(deliveryMessage(0)); err != nil {
					t.Fatalf("storeMessage(0) error = %v, want nil", err)
				}
				synctest.Wait()
				receiveDelivery(t, inbound, 0, deliveryMessage(0))
				before := subscriptionDeadline(t, target, "subscriber")
				<-time.After(time.Second)
				err := manager.Acknowledgement(t.Context(), *protocol.NewClientMetadata(inbound.IPCPath()), *protocol.NewAcknowledgementPayload("subscriber", offset))
				if !errors.Is(err, protocol.ErrInvalidPayload) {
					t.Errorf("Acknowledgement(subscriber, %d) error = %v, want %v", offset, err, protocol.ErrInvalidPayload)
				}
				if got := subscriptionDeadline(t, target, "subscriber"); !got.Equal(before) {
					t.Errorf("deadline after rejected ACK(%d) = %v, want %v", offset, got, before)
				}
				checkSavedOffset(t, manager, "subscriber", 0)
			})
		})
	}
}

// TestConsumerHeartbeatRejectsReplacedIPC checks a notification from an earlier connection.
func TestConsumerHeartbeatRejectsReplacedIPC(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	first := newTopicManagerInbox(t)
	stopFirst := subscribeForDelivery(t, manager, lifetime, first, "subscriber", "", nil)
	stopFirst()
	if !waitTopicWorkers(t, target) {
		return
	}
	second := expirationInbox(t, "replacement")
	subscribeForDelivery(t, manager, lifetime, second, "subscriber", "", nil)
	err := manager.ConsumerHeartbeat(t.Context(), *protocol.NewClientMetadata(first.IPCPath()), *protocol.NewConsumerHeartbeatPayload("subscriber"))
	if !errors.Is(err, consumer.ErrNotFound) {
		t.Errorf("ConsumerHeartbeat(subscriber) from replaced IPC error = %v, want %v", err, consumer.ErrNotFound)
	}
	if err := manager.ConsumerHeartbeat(t.Context(), *protocol.NewClientMetadata(second.IPCPath()), *protocol.NewConsumerHeartbeatPayload("subscriber")); err != nil {
		t.Errorf("ConsumerHeartbeat(subscriber) from current IPC error = %v, want nil", err)
	}
}

// TestConsumerNotificationsRejectExpiredDeadline checks late activity before timer dispatch.
func TestConsumerNotificationsRejectExpiredDeadline(t *testing.T) {
	tests := []struct {
		name   string
		notify func(*Manager, context.Context, protocol.ClientMetadata) error
	}{
		{name: "heartbeat", notify: func(m *Manager, ctx context.Context, metadata protocol.ClientMetadata) error {
			return m.ConsumerHeartbeat(ctx, metadata, *protocol.NewConsumerHeartbeatPayload("subscriber"))
		}},
		{name: "acknowledgement", notify: func(m *Manager, ctx context.Context, metadata protocol.ClientMetadata) error {
			return m.Acknowledgement(ctx, metadata, *protocol.NewAcknowledgementPayload("subscriber", 1))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				manager := NewManager()
				target, lifetime, _ := subscriptionTarget(t, manager, "events")
				inbound := expirationInbox(t, "consumer")
				subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "", nil)
				if err := target.storeMessage(deliveryMessage(0)); err != nil {
					t.Fatalf("storeMessage(0) error = %v, want nil", err)
				}
				synctest.Wait()
				receiveDelivery(t, inbound, 0, deliveryMessage(0))
				connection := subscribedConnection(t, target, "subscriber")
				target.mu.Lock()
				c, _ := target.consumerMap.Get("subscriber")
				c.liveness.deadline = time.Now()
				target.mu.Unlock()
				err := tt.notify(manager, t.Context(), *protocol.NewClientMetadata(inbound.IPCPath()))
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("%s(subscriber) at its elapsed deadline error = %v, want %v", tt.name, err, context.DeadlineExceeded)
				}
				synctest.Wait()
				select {
				case <-connection.Done():
				default:
					t.Errorf("Done(subscriber) after expired %s is open, want closed", tt.name)
				}
				checkSavedOffset(t, manager, "subscriber", 0)
			})
		})
	}
}

// TestConsumerExpirationCancelsBlockedDelivery checks timeout cleanup while the queue is full.
func TestConsumerExpirationCancelsBlockedDelivery(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	inbound := expirationInbox(t, "consumer")
	entered := make(chan struct{})
	consumerCtx := subscriptionReplyContext{
		Context:    lifetime,
		beforeSend: sync.OnceFunc(func() { close(entered) }),
	}
	subscribeForDelivery(t, manager, consumerCtx, inbound, "subscriber", "", nil)
	fillSubscriptionInbox(t, inbound)
	if err := target.storeMessage(deliveryMessage(0)); err != nil {
		t.Fatalf("storeMessage(0) with a full consumer queue error = %v, want nil", err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Error("delivery to a full consumer queue did not start within 5s, want started")
		return
	}
	connection := subscribedConnection(t, target, "subscriber")
	target.mu.Lock()
	c, _ := target.consumerMap.Get("subscriber")
	c.liveness.deadline = time.Now()
	c.liveness.changed <- struct{}{}
	target.mu.Unlock()
	if !waitTopicWorkers(t, target) {
		return
	}
	select {
	case <-connection.Done():
	default:
		t.Error("Done(subscriber) after expiration during blocked delivery is open, want closed")
	}
	checkSavedOffset(t, manager, "subscriber", 0)
	replacement := expirationInbox(t, "consumer")
	subscribeForDelivery(t, manager, lifetime, replacement, "subscriber", "", nil)
	receiveDelivery(t, replacement, 0, deliveryMessage(0))
}

// TestManagerConsumerInfoRepliesWithStoredMetadata checks current state without connection changes.
func TestManagerConsumerInfoRepliesWithStoredMetadata(t *testing.T) {
	tests := []struct {
		name      string
		count     int
		offset    uint32
		prefix    string
		connected bool
	}{
		{name: "zero offset", count: 1},
		{name: "saved consumer", count: 1, offset: 17, prefix: "é/猫/"},
		{name: "connected consumer", count: 1, offset: 42, prefix: "user/", connected: true},
		{name: "large registry", count: 128, offset: ^uint32(0), prefix: "events/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			inbound := newTopicManagerInbox(t)
			subscriberIPC := "/saved-consumer"
			if tt.connected {
				subscriber, err := ipc.NewInbound(inbound.IPCPath()+".subscriber", protocol.Decode)
				if err != nil {
					t.Fatalf("NewInbound(subscriber) during setup error = %v, want nil", err)
				}
				subscriberIPC = subscriber.IPCPath()
				t.Cleanup(func() {
					if err := subscriber.Drop(); err != nil {
						t.Errorf("Drop(subscriber) error = %v, want nil", err)
					}
				})
			}
			for i := 0; i < tt.count; i++ {
				consumerID := id.ID(fmt.Sprintf("consumer-%03d", i))
				if err := manager.consumerManager.AddConsumer(consumerID, "events", 7, tt.prefix, subscriberIPC); err != nil {
					t.Fatalf("AddConsumer(%q) during setup error = %v, want nil", consumerID, err)
				}
			}
			consumerID := id.ID(fmt.Sprintf("consumer-%03d", tt.count-1))
			c, err := manager.consumerManager.GetConsumer(consumerID)
			if err != nil {
				t.Fatalf("GetConsumer(%q) during setup error = %v, want nil", consumerID, err)
			}
			if tt.connected {
				if err := manager.consumerManager.ConnectConsumer(ctx, c); err != nil {
					t.Fatalf("ConnectConsumer(%q) during setup error = %v, want nil", consumerID, err)
				}
				t.Cleanup(func() {
					if err := manager.consumerManager.DisconnectConsumer(c); err != nil {
						t.Errorf("DisconnectConsumer(%q) error = %v, want nil", consumerID, err)
					}
				})
			}
			if err := manager.consumerManager.UpdateInfo(consumerID, nil, &tt.offset, nil, nil); err != nil {
				t.Fatalf("UpdateInfo(%q, offset %d) during setup error = %v, want nil", consumerID, tt.offset, err)
			}
			payload := *protocol.NewConsumerInfoPayload(string(consumerID))
			if err := manager.ConsumerInfo(ctx, *protocol.NewClientMetadata(inbound.IPCPath()), payload); err != nil {
				t.Errorf("ConsumerInfo(%q) error = %v, want nil", consumerID, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ConsumerInfoReply)
			reply, ok := data.Payload().(*protocol.ConsumerInfoReplyPayload)
			if !ok {
				t.Errorf("ConsumerInfo(%q) payload = %T, want *protocol.ConsumerInfoReplyPayload", consumerID, data.Payload())
			} else if reply.Client() != string(consumerID) || reply.Topic() != "events" || reply.Offset() != tt.offset || reply.Prefix() != tt.prefix || reply.IPC() != subscriberIPC {
				t.Errorf("ConsumerInfo(%q) payload = %+v, want events, offset %d, prefix %q and IPC %q", consumerID, reply, tt.offset, tt.prefix, subscriberIPC)
			}
			stored, err := manager.consumerManager.GetConsumer(consumerID)
			if err != nil {
				t.Errorf("GetConsumer(%q) after ConsumerInfo error = %v, want nil", consumerID, err)
			} else if stored.Info.Topic() != "events" || stored.Info.Offset() != tt.offset || stored.Info.Prefix() != tt.prefix || stored.Info.IPCPath() != subscriberIPC {
				t.Errorf("stored %q after ConsumerInfo = %+v, want events, offset %d, prefix %q and IPC %q", consumerID, stored.Info, tt.offset, tt.prefix, subscriberIPC)
			}
			if got := manager.consumerManager.Len(); got != uint64(tt.count) {
				t.Errorf("consumer count after ConsumerInfo(%q) = %d, want %d", consumerID, got, tt.count)
			}
			if connection := c.Connection(); tt.connected {
				if connection == nil {
					t.Errorf("connection for %q after ConsumerInfo = nil, want open", consumerID)
					return
				}
				select {
				case <-connection.Done():
					t.Errorf("connection for %q after ConsumerInfo is closed, want open", consumerID)
				default:
				}
			} else if connection != nil {
				t.Errorf("connection for %q after ConsumerInfo = %p, want nil", consumerID, connection)
			}
		})
	}
}

// TestManagerConsumerInfoRejectsUnknownSubscriber checks lookup failure without adding metadata.
func TestManagerConsumerInfoRejectsUnknownSubscriber(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "empty registry"},
		{name: "one consumer", count: 1},
		{name: "large registry", count: 128},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			for i := 0; i < tt.count; i++ {
				consumerID := id.ID(fmt.Sprintf("consumer-%03d", i))
				if err := manager.consumerManager.AddConsumer(consumerID, "events", 17, "user/", "/saved-consumer"); err != nil {
					t.Fatalf("AddConsumer(%q) during setup error = %v, want nil", consumerID, err)
				}
			}
			ctx := topicManagerContext(t)
			inbound := newTopicManagerInbox(t)
			payload := *protocol.NewConsumerInfoPayload("missing")
			if err := manager.ConsumerInfo(ctx, *protocol.NewClientMetadata(inbound.IPCPath()), payload); err != nil {
				t.Errorf("ConsumerInfo(missing) error = %v, want nil after rejection", err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ErrorReply)
			reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
			if !ok {
				t.Errorf("ConsumerInfo(missing) payload = %T, want *protocol.ErrorReplyPayload", data.Payload())
			} else if reply.Kind() != protocol.ClientNotFound || reply.Reason() == "" {
				t.Errorf("ConsumerInfo(missing) rejection = %v, %q, want %v and a nonempty reason", reply.Kind(), reply.Reason(), protocol.ClientNotFound)
			}
			if got := manager.consumerManager.Len(); got != uint64(tt.count) {
				t.Errorf("consumer count after ConsumerInfo(missing) = %d, want %d", got, tt.count)
			}
		})
	}
}

// TestManagerConsumerInfoPropagatesQueueErrors checks both reply paths.
func TestManagerConsumerInfoPropagatesQueueErrors(t *testing.T) {
	missingIPC := fmt.Sprintf("/consumer-info.missing.%d", os.Getpid())
	tests := []struct {
		name     string
		existing bool
		ipcPath  string
		wantErr  error
	}{
		{name: "known invalid queue", existing: true, ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "known missing queue", existing: true, ipcPath: missingIPC, wantErr: os.ErrNotExist},
		{name: "unknown invalid queue", ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "unknown missing queue", ipcPath: missingIPC, wantErr: os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			if tt.existing {
				if err := manager.consumerManager.AddConsumer("subscriber", "events", 17, "user/", "/saved-consumer"); err != nil {
					t.Fatalf("AddConsumer(subscriber) during setup error = %v, want nil", err)
				}
			}
			payload := *protocol.NewConsumerInfoPayload("subscriber")
			if err := manager.ConsumerInfo(topicManagerContext(t), *protocol.NewClientMetadata(tt.ipcPath), payload); !errors.Is(err, tt.wantErr) {
				t.Errorf("ConsumerInfo(subscriber) with reply queue %q error = %v, want %v", tt.ipcPath, err, tt.wantErr)
			}
		})
	}
}

// TestManagerConsumerInfoPropagatesStoppedContexts checks cancellation of both reply paths.
func TestManagerConsumerInfoPropagatesStoppedContexts(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, expireCancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer expireCancel()
	tests := []struct {
		name     string
		existing bool
		ctx      context.Context
		wantErr  error
	}{
		{name: "known canceled", existing: true, ctx: canceled, wantErr: context.Canceled},
		{name: "known expired", existing: true, ctx: expired, wantErr: context.DeadlineExceeded},
		{name: "unknown canceled", ctx: canceled, wantErr: context.Canceled},
		{name: "unknown expired", ctx: expired, wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			if tt.existing {
				if err := manager.consumerManager.AddConsumer("subscriber", "events", 17, "user/", "/saved-consumer"); err != nil {
					t.Fatalf("AddConsumer(subscriber) during setup error = %v, want nil", err)
				}
			}
			inbound := newTopicManagerInbox(t)
			payload := *protocol.NewConsumerInfoPayload("subscriber")
			err := manager.ConsumerInfo(tt.ctx, *protocol.NewClientMetadata(inbound.IPCPath()), payload)
			if !errors.Is(err, tt.wantErr) || !errors.Is(err, response.ErrReply) {
				t.Errorf("ConsumerInfo(subscriber) with %s error = %v, want %v and %v", tt.name, err, tt.wantErr, response.ErrReply)
			}
		})
	}
}

// TestManagerConsumerInfoTimesOutOnFullQueue checks bounded delivery for both replies.
func TestManagerConsumerInfoTimesOutOnFullQueue(t *testing.T) {
	tests := []struct {
		name     string
		existing bool
	}{
		{name: "known subscriber", existing: true},
		{name: "unknown subscriber"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			if tt.existing {
				if err := manager.consumerManager.AddConsumer("subscriber", "events", 17, "user/", "/saved-consumer"); err != nil {
					t.Fatalf("AddConsumer(subscriber) during setup error = %v, want nil", err)
				}
			}
			inbound := newTopicManagerInbox(t)
			fillSubscriptionInbox(t, inbound)
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			payload := *protocol.NewConsumerInfoPayload("subscriber")
			err := manager.ConsumerInfo(ctx, *protocol.NewClientMetadata(inbound.IPCPath()), payload)
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, response.ErrReply) {
				t.Errorf("ConsumerInfo(subscriber) with a full reply queue error = %v, want %v and %v", err, context.DeadlineExceeded, response.ErrReply)
			}
		})
	}
}

// TestManagerConsumerInfoPropagatesLookupErrors checks internal errors are not client rejections.
func TestManagerConsumerInfoPropagatesLookupErrors(t *testing.T) {
	manager := NewManager()
	manager.consumerManager = nil
	payload := *protocol.NewConsumerInfoPayload("subscriber")
	err := manager.ConsumerInfo(t.Context(), *protocol.NewClientMetadata("/invalid/queue"), payload)
	if !errors.Is(err, consumer.ErrManagerUninitialized) {
		t.Errorf("ConsumerInfo(subscriber) with an uninitialized consumer manager error = %v, want %v", err, consumer.ErrManagerUninitialized)
	}
}

// TestManagerAcknowledgementRejectsMissingConnections checks one-way routing failures.
func TestManagerAcknowledgementRejectsMissingConnections(t *testing.T) {
	tests := []struct {
		name     string
		existing bool
		topic    bool
	}{
		{name: "unknown consumer"},
		{name: "missing topic", existing: true},
		{name: "disconnected consumer", existing: true, topic: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			if tt.topic {
				subscriptionTarget(t, manager, "events")
			}
			if tt.existing {
				if err := manager.consumerManager.AddConsumer("subscriber", "events", 17, "", "/subscriber"); err != nil {
					t.Fatalf("AddConsumer(subscriber) during setup error = %v, want nil", err)
				}
			}
			payload := *protocol.NewAcknowledgementPayload("subscriber", 18)
			err := manager.Acknowledgement(t.Context(), *protocol.NewClientMetadata("/subscriber"), payload)
			if !errors.Is(err, consumer.ErrNotFound) {
				t.Errorf("Acknowledgement(subscriber, 18) with %s error = %v, want %v", tt.name, err, consumer.ErrNotFound)
			}
		})
	}
}

// TestManagerConsumerDisconnectRejectsMissingConnections checks one-way routing failures.
func TestManagerConsumerDisconnectRejectsMissingConnections(t *testing.T) {
	tests := []struct {
		name     string
		existing bool
		topic    bool
	}{
		{name: "unknown consumer"},
		{name: "missing topic", existing: true},
		{name: "disconnected consumer", existing: true, topic: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			if tt.topic {
				subscriptionTarget(t, manager, "events")
			}
			if tt.existing {
				if err := manager.consumerManager.AddConsumer("subscriber", "events", 17, "user/", "/subscriber"); err != nil {
					t.Fatalf("AddConsumer(subscriber) during setup error = %v, want nil", err)
				}
			}
			err := manager.ConsumerDisconnect(t.Context(), *protocol.NewClientMetadata("/subscriber"), *protocol.NewConsumerDisconnectPayload("subscriber"))
			if !errors.Is(err, consumer.ErrNotFound) {
				t.Errorf("ConsumerDisconnect(subscriber) with %s error = %v, want %v", tt.name, err, consumer.ErrNotFound)
			}
			if tt.existing {
				c, err := manager.consumerManager.GetConsumer("subscriber")
				if err != nil || c.Info.Offset() != 17 || c.Info.Topic() != "events" || c.Info.Prefix() != "user/" || c.Info.IPCPath() != "/subscriber" {
					t.Errorf("saved subscriber after rejected disconnect = %+v, %v, want events, offset 17, user/, /subscriber and nil", c, err)
				}
			}
		})
	}
}

// TestManagerConsumerDisconnectPropagatesLookupErrors checks the existing manager error contract.
func TestManagerConsumerDisconnectPropagatesLookupErrors(t *testing.T) {
	manager := NewManager()
	manager.consumerManager = nil
	err := manager.ConsumerDisconnect(t.Context(), *protocol.NewClientMetadata("/subscriber"), *protocol.NewConsumerDisconnectPayload("subscriber"))
	if !errors.Is(err, consumer.ErrManagerUninitialized) {
		t.Errorf("ConsumerDisconnect(subscriber) with an uninitialized consumer manager error = %v, want %v", err, consumer.ErrManagerUninitialized)
	}
}

// TestManagerConsumerDisconnectRejectsStoppedContexts checks cancellation before detachment.
func TestManagerConsumerDisconnectRejectsStoppedContexts(t *testing.T) {
	tests := []struct {
		name    string
		expired bool
		wantErr error
	}{
		{name: "canceled", wantErr: context.Canceled},
		{name: "expired", expired: true, wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			target, lifetime, _ := subscriptionTarget(t, manager, "events")
			inbound := deliveryInbox(t, "consumer")
			subscribeForDelivery(t, manager, lifetime, inbound, "subscriber", "", nil)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if tt.expired {
				ctx, cancel = context.WithDeadline(t.Context(), time.Unix(0, 0))
				defer cancel()
			}
			err := manager.ConsumerDisconnect(ctx, *protocol.NewClientMetadata(inbound.IPCPath()), *protocol.NewConsumerDisconnectPayload("subscriber"))
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("ConsumerDisconnect(subscriber) with %s context error = %v, want %v", tt.name, err, tt.wantErr)
			}
			current := subscriptionSnapshot(t, target, "subscriber")
			select {
			case <-current.connection.Done():
				t.Errorf("Done(subscriber) after rejected %s disconnect is closed, want open", tt.name)
			default:
			}
		})
	}
}

// TestManagerConsumerDisconnectRejectsReplacedIPC checks that an old client cannot stop its replacement.
func TestManagerConsumerDisconnectRejectsReplacedIPC(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	first := deliveryInbox(t, "first")
	subscribeForDelivery(t, manager, lifetime, first, "subscriber", "", nil)
	payload := *protocol.NewConsumerDisconnectPayload("subscriber")
	if err := manager.ConsumerDisconnect(topicManagerContext(t), *protocol.NewClientMetadata(first.IPCPath()), payload); err != nil {
		t.Fatalf("ConsumerDisconnect(subscriber) for the original IPC error = %v, want nil", err)
	}
	second := deliveryInbox(t, "second")
	subscribeForDelivery(t, manager, lifetime, second, "subscriber", "", nil)
	err := manager.ConsumerDisconnect(topicManagerContext(t), *protocol.NewClientMetadata(first.IPCPath()), payload)
	if !errors.Is(err, consumer.ErrNotFound) {
		t.Errorf("ConsumerDisconnect(subscriber) from replaced IPC error = %v, want %v", err, consumer.ErrNotFound)
	}
	if err := target.ProduceMessage(topicManagerContext(t), deliveryMessage(0)); err != nil {
		t.Fatalf("ProduceMessage(0) after rejecting old IPC error = %v, want nil", err)
	}
	receiveDelivery(t, second, 0, deliveryMessage(0))
	acknowledgeDelivery(t, manager, second, "subscriber", 1)
}

// TestManagerNewSubscriptionRejectsShutdown checks admission without metadata changes.
func TestManagerNewSubscriptionRejectsShutdown(t *testing.T) {
	tests := []struct {
		name     string
		existing bool
	}{
		{name: "new consumer"},
		{name: "saved consumer", existing: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			target, lifetime, _ := subscriptionTarget(t, manager, "events")
			if tt.existing {
				if err := manager.consumerManager.AddConsumer("consumer-1", "old", 17, "old/", "/old"); err != nil {
					t.Errorf("AddConsumer(consumer-1, old, 17) during setup error = %v, want nil", err)
					return
				}
			}
			wantCount := manager.consumerManager.Len()
			requested := make(chan struct{})
			shutdown := lifecycle.NewGracefulShutdown(requested)
			close(requested)
			ctx := topicManagerContext(t)
			inbound := newTopicManagerInbox(t)
			offset := uint32(42)
			payload := *protocol.NewTopicSubscriptionPayload("consumer-1", "events", "new/", &offset)
			if err := manager.NewSubscription(ctx, lifetime, shutdown, *protocol.NewClientMetadata(inbound.IPCPath()), payload); err != nil {
				t.Errorf("NewSubscription(consumer-1, events) during shutdown error = %v, want nil after rejection", err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ServerShutdown)
			if _, ok := data.Payload().(*protocol.ServerShutdownPayload); !ok {
				t.Errorf("NewSubscription() during shutdown payload = %T, want *protocol.ServerShutdownPayload", data.Payload())
			}
			if got := manager.consumerManager.Len(); got != wantCount {
				t.Errorf("consumer count after rejected subscription during shutdown = %d, want %d", got, wantCount)
			}
			stored, err := manager.consumerManager.GetConsumer("consumer-1")
			if tt.existing {
				if err != nil || stored.Info.Topic() != "old" || stored.Info.Offset() != 17 || stored.Info.Prefix() != "old/" || stored.Info.IPCPath() != "/old" {
					t.Errorf("saved metadata after rejected subscription during shutdown = %+v, %v, want old, offset 17, prefix old/, queue /old and nil", stored, err)
				}
			} else if !errors.Is(err, consumer.ErrNotFound) {
				t.Errorf("GetConsumer(consumer-1) after rejected new subscription during shutdown error = %v, want %v", err, consumer.ErrNotFound)
			}
			target.mu.Lock()
			attached := target.consumerMap.Len()
			target.mu.Unlock()
			if attached != 0 {
				t.Errorf("attached consumer count after rejected subscription during shutdown = %d, want 0", attached)
			}
		})
	}
}

// TestManagerNewSubscriptionFinishesDuringShutdown checks confirmation after admission.
func TestManagerNewSubscriptionFinishesDuringShutdown(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	requested := make(chan struct{})
	shutdown := lifecycle.NewGracefulShutdown(requested)
	ctx := topicManagerContext(t)
	replyCtx := subscriptionReplyContext{
		Context:    ctx,
		beforeSend: sync.OnceFunc(func() { close(requested) }),
	}
	inbound := newTopicManagerInbox(t)
	payload := *protocol.NewTopicSubscriptionPayload("consumer-1", "events", "user/", nil)
	if err := manager.NewSubscription(replyCtx, lifetime, shutdown, *protocol.NewClientMetadata(inbound.IPCPath()), payload); err != nil {
		t.Errorf("NewSubscription(consumer-1, events) admitted before shutdown error = %v, want nil", err)
		return
	}
	if !shutdown.IsRequested() {
		t.Error("shutdown during subscription confirmation was not requested, want requested")
	}
	receiveTopicManagerReply(t, ctx, inbound, protocol.TopicSubscriptionReply)
	target.mu.Lock()
	stored, exists := target.consumerMap.Get("consumer-1")
	var connection *consumer.Connection
	if exists && stored != nil {
		connection = stored.connection
	}
	target.mu.Unlock()
	if connection == nil {
		t.Error("connection for subscription admitted before shutdown = nil, want an open connection")
		return
	}
	select {
	case <-connection.Done():
		t.Error("subscription connection after shutdown request is closed, want open while its context is active")
	default:
	}
}

// TestManagerNewSubscriptionConfirmsAttachment checks metadata and connection lifetime.
func TestManagerNewSubscriptionConfirmsAttachment(t *testing.T) {
	requested := uint32(42)
	tests := []struct {
		name     string
		prefix   string
		offset   *uint32
		existing bool
		want     uint32
	}{
		{name: "first subscription"},
		{name: "explicit offset", prefix: "user/", offset: &requested, want: requested},
		{name: "resume", prefix: "é/猫/", existing: true, want: 17},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			target, lifetime, _ := subscriptionTarget(t, manager, "events")
			inbound := newTopicManagerInbox(t)
			if tt.existing {
				if err := manager.consumerManager.AddConsumer("consumer-1", "events", 17, "old/", "/old"); err != nil {
					t.Fatalf("AddConsumer(consumer-1, events, 17) during setup error = %v, want nil", err)
				}
			}
			replyCtx, cancelReply := context.WithCancel(topicManagerContext(t))
			defer cancelReply()
			payload := *protocol.NewTopicSubscriptionPayload("consumer-1", "events", tt.prefix, tt.offset)
			if err := manager.NewSubscription(replyCtx, lifetime, lifecycle.GracefulShutdown{}, *protocol.NewClientMetadata(inbound.IPCPath()), payload); err != nil {
				t.Fatalf("NewSubscription(consumer-1, events, %q) error = %v, want nil", tt.prefix, err)
			}
			data := receiveTopicManagerReply(t, replyCtx, inbound, protocol.TopicSubscriptionReply)
			if _, ok := data.Payload().(*protocol.TopicSubscriptionReplyPayload); !ok {
				t.Errorf("subscription confirmation payload = %T, want *protocol.TopicSubscriptionReplyPayload", data.Payload())
			}
			target.mu.Lock()
			stored, exists := target.consumerMap.Get("consumer-1")
			var snapshot subscription
			if exists && stored != nil {
				snapshot = *stored
			}
			target.mu.Unlock()
			if !exists || snapshot.connection == nil {
				t.Fatalf("topic attachment for consumer-1 = %+v, exists %t, want a connection and true", snapshot, exists)
			}
			if snapshot.info.Topic() != "events" || snapshot.info.Offset() != tt.want || snapshot.info.Prefix() != tt.prefix || snapshot.info.IPCPath() != inbound.IPCPath() {
				t.Errorf("attached consumer-1 = %+v, want events, offset %d, prefix %q, queue %q", snapshot.info, tt.want, tt.prefix, inbound.IPCPath())
			}
			cancelReply()
			select {
			case <-snapshot.connection.Done():
				t.Error("subscription connection after reply context cancellation is closed, want open")
			default:
			}
		})
	}
}

// TestManagerNewSubscriptionWaitsForConfirmation checks workers during reply transmission.
func TestManagerNewSubscriptionWaitsForConfirmation(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	inbound := newTopicManagerInbox(t)
	ctx := topicManagerContext(t)
	var connection *consumer.Connection
	replyCtx := subscriptionReplyContext{
		Context: ctx,
		beforeSend: sync.OnceFunc(func() {
			target.mu.Lock()
			stored, exists := target.consumerMap.Get("consumer-1")
			if exists && stored != nil {
				connection = stored.connection
			}
			target.mu.Unlock()
			if connection == nil {
				t.Error("consumer-1 attachment before confirmation = nil, want a registered connection")
				return
			}
			select {
			case connection.HeartbeatChan <- struct{}{}:
				t.Error("consumer-1 heartbeat before confirmation was consumed, want pending")
			default:
			}
		}),
	}
	payload := *protocol.NewTopicSubscriptionPayload("consumer-1", "events", "user/", nil)
	if err := manager.NewSubscription(replyCtx, lifetime, lifecycle.GracefulShutdown{}, *protocol.NewClientMetadata(inbound.IPCPath()), payload); err != nil {
		t.Errorf("NewSubscription(consumer-1, events) error = %v, want nil", err)
		return
	}
	receiveTopicManagerReply(t, ctx, inbound, protocol.TopicSubscriptionReply)
	if connection == nil {
		return
	}
	select {
	case connection.HeartbeatChan <- struct{}{}:
	case <-ctx.Done():
		t.Errorf("consumer-1 heartbeat after confirmation error = %v, want consumed before deadline", ctx.Err())
	}
}

// TestManagerNewSubscriptionReconnects checks saved offsets and changed subscriptions.
func TestManagerNewSubscriptionReconnects(t *testing.T) {
	zero, requested := uint32(0), uint32(42)
	tests := []struct {
		name   string
		topic  string
		offset *uint32
		want   uint32
	}{
		{name: "same topic", topic: "events", want: 17},
		{name: "changed topic", topic: "orders"},
		{name: "explicit rewind", topic: "orders", offset: &zero},
		{name: "explicit offset", topic: "orders", offset: &requested, want: requested},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			first, firstCtx, _ := subscriptionTarget(t, manager, "events")
			firstLifetime, stopFirst := context.WithCancel(firstCtx)
			t.Cleanup(stopFirst)
			_, secondCtx, _ := subscriptionTarget(t, manager, "orders")
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			ctx := topicManagerContext(t)
			if err := manager.NewSubscription(ctx, firstLifetime, lifecycle.GracefulShutdown{}, metadata, *protocol.NewTopicSubscriptionPayload("consumer-1", "events", "old/", nil)); err != nil {
				t.Fatalf("first NewSubscription(consumer-1, events) error = %v, want nil", err)
			}
			receiveTopicManagerReply(t, ctx, inbound, protocol.TopicSubscriptionReply)
			offset := uint32(17)
			if err := manager.consumerManager.UpdateInfo("consumer-1", nil, &offset, nil, nil); err != nil {
				t.Fatalf("UpdateInfo(consumer-1, nil, 17, nil, nil) during setup error = %v, want nil", err)
			}
			stopFirst()
			if !waitTopicWorkers(t, first) {
				return
			}
			if err := manager.NewSubscription(ctx, secondCtx, lifecycle.GracefulShutdown{}, metadata, *protocol.NewTopicSubscriptionPayload("consumer-1", tt.topic, "new/", tt.offset)); err != nil {
				t.Fatalf("repeated NewSubscription(consumer-1, %q) error = %v, want nil", tt.topic, err)
			}
			receiveTopicManagerReply(t, ctx, inbound, protocol.TopicSubscriptionReply)
			stored, err := manager.consumerManager.GetConsumer("consumer-1")
			if err != nil {
				t.Fatalf("GetConsumer(consumer-1) after reconnect error = %v, want nil", err)
			}
			if stored.Info.Topic() != tt.topic || stored.Info.Offset() != tt.want || stored.Info.Prefix() != "new/" {
				t.Errorf("reconnected consumer-1 = %+v, want topic %q, offset %d and prefix new/", stored.Info, tt.topic, tt.want)
			}
			if tt.topic != "events" {
				first.mu.Lock()
				count := first.consumerMap.Len()
				first.mu.Unlock()
				if count != 0 {
					t.Errorf("old topic consumer count after moving to %q = %d, want 0", tt.topic, count)
				}
			}
		})
	}
}

// TestManagerNewSubscriptionRejectsRequests checks missing topics.
func TestManagerNewSubscriptionRejectsRequests(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
		topic    string
		prefix   string
		wantKind protocol.ErrorKind
	}{
		{name: "missing topic", clientID: "consumer-1", topic: "missing", wantKind: protocol.TopicNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			_, lifetime, _ := subscriptionTarget(t, manager, "events")
			inbound := newTopicManagerInbox(t)
			ctx := topicManagerContext(t)
			payload := *protocol.NewTopicSubscriptionPayload(tt.clientID, tt.topic, tt.prefix, nil)
			if err := manager.NewSubscription(ctx, lifetime, lifecycle.GracefulShutdown{}, *protocol.NewClientMetadata(inbound.IPCPath()), payload); err != nil {
				t.Errorf("NewSubscription(%q, %q) error = %v, want nil after rejection", tt.clientID, tt.topic, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ErrorReply)
			reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
			if !ok {
				t.Errorf("NewSubscription(%q, %q) payload = %T, want *protocol.ErrorReplyPayload", tt.clientID, tt.topic, data.Payload())
			} else if reply.Kind() != tt.wantKind || reply.Reason() == "" {
				t.Errorf("NewSubscription(%q, %q) rejection = %v, %q, want %v and a nonempty reason", tt.clientID, tt.topic, reply.Kind(), reply.Reason(), tt.wantKind)
			}
			if got := manager.consumerManager.Len(); got != 0 {
				t.Errorf("consumer count after rejected subscription = %d, want 0", got)
			}
		})
	}
}

// TestManagerNewSubscriptionRejectsActiveName checks preservation of an active consumer.
func TestManagerNewSubscriptionRejectsActiveName(t *testing.T) {
	manager := NewManager()
	_, lifetime, _ := subscriptionTarget(t, manager, "events")
	subscriptionTarget(t, manager, "orders")
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	ctx := topicManagerContext(t)
	if err := manager.NewSubscription(ctx, lifetime, lifecycle.GracefulShutdown{}, metadata, *protocol.NewTopicSubscriptionPayload("consumer-1", "events", "old/", nil)); err != nil {
		t.Fatalf("first NewSubscription(consumer-1) error = %v, want nil", err)
	}
	receiveTopicManagerReply(t, ctx, inbound, protocol.TopicSubscriptionReply)
	if err := manager.NewSubscription(ctx, lifetime, lifecycle.GracefulShutdown{}, metadata, *protocol.NewTopicSubscriptionPayload("consumer-1", "orders", "new/", nil)); err != nil {
		t.Fatalf("duplicate NewSubscription(consumer-1) error = %v, want nil after rejection", err)
	}
	data := receiveTopicManagerReply(t, ctx, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok || reply.Kind() != protocol.ClientNameTaken {
		t.Errorf("duplicate subscription payload = %+v, want %v rejection", data.Payload(), protocol.ClientNameTaken)
	}
	stored, err := manager.consumerManager.GetConsumer("consumer-1")
	if err != nil || stored.Info.Topic() != "events" || stored.Info.Prefix() != "old/" {
		t.Errorf("metadata after duplicate subscription = %+v, %v, want events, old/ and nil", stored, err)
	}
}

// TestManagerNewSubscriptionRestoresFailedAttachment checks index failure cleanup.
func TestManagerNewSubscriptionRestoresFailedAttachment(t *testing.T) {
	manager := NewManager()
	target, lifetime, _ := subscriptionTarget(t, manager, "events")
	target.consumerTrie = &ConsumerTrie{}
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	ctx := topicManagerContext(t)
	payload := *protocol.NewTopicSubscriptionPayload("consumer-1", "events", "user/", nil)
	if err := manager.NewSubscription(ctx, lifetime, lifecycle.GracefulShutdown{}, metadata, payload); !errors.Is(err, ErrConsumerTrieUninitialized) {
		t.Errorf("NewSubscription() with invalid prefix index error = %v, want %v", err, ErrConsumerTrieUninitialized)
	}
	if got := manager.consumerManager.Len(); got != 0 {
		t.Errorf("consumer count after failed attachment = %d, want 0", got)
	}
	target.consumerTrie = NewConsumerTrie()
	if err := manager.NewSubscription(ctx, lifetime, lifecycle.GracefulShutdown{}, metadata, payload); err != nil {
		t.Errorf("NewSubscription() after repairing the index error = %v, want nil", err)
		return
	}
	receiveTopicManagerReply(t, ctx, inbound, protocol.TopicSubscriptionReply)
}

func fillSubscriptionInbox(t *testing.T, inbound *ipc.Inbound) int {
	t.Helper()
	data, err := protocol.NewData(protocol.ServerShutdown, protocol.NewServerMetadata(), protocol.NewServerShutdownPayload())
	if err != nil {
		t.Fatalf("NewData(ServerShutdown) error = %v, want nil", err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(ServerShutdown) error = %v, want nil", err)
	}
	sender, err := mqueue.NewSender(inbound.IPCPath())
	if err != nil {
		t.Fatalf("NewSender(%q) error = %v, want nil", inbound.IPCPath(), err)
	}
	defer func() {
		if err := sender.Close(); err != nil {
			t.Errorf("Close(%q) after filling the queue error = %v, want nil", inbound.IPCPath(), err)
		}
	}()
	for count := 0; ; count++ {
		err := sender.TimedSend(frame, time.Now())
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return count
		}
		if err != nil {
			t.Fatalf("TimedSend(ServerShutdown) to %q at frame %d error = %v, want nil or %v", inbound.IPCPath(), count, err, os.ErrDeadlineExceeded)
		}
	}
}

// TestManagerNewSubscriptionRestoresFailedReply checks rollback after confirmation failure.
func TestManagerNewSubscriptionRestoresFailedReply(t *testing.T) {
	requested := uint32(42)
	tests := []struct {
		name     string
		existing bool
		offset   *uint32
	}{
		{name: "new consumer", offset: &requested},
		{name: "saved consumer explicit offset", existing: true, offset: &requested},
		{name: "saved consumer default offset", existing: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			target, lifetime, _ := subscriptionTarget(t, manager, "events")
			if tt.existing {
				if err := manager.consumerManager.AddConsumer("consumer-1", "old", 17, "old/", "/old"); err != nil {
					t.Fatalf("AddConsumer(consumer-1, old, 17) error = %v, want nil", err)
				}
			}
			inbound := newTopicManagerInbox(t)
			count := fillSubscriptionInbox(t, inbound)
			ctx := topicManagerContext(t)
			replyCtx, cancelReply := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancelReply()
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			payload := *protocol.NewTopicSubscriptionPayload("consumer-1", "events", "new/", tt.offset)
			shutdownRequested := make(chan struct{})
			shutdown := lifecycle.NewGracefulShutdown(shutdownRequested)
			replyCtx = subscriptionReplyContext{
				Context:    replyCtx,
				beforeSend: sync.OnceFunc(func() { close(shutdownRequested) }),
			}
			if err := manager.NewSubscription(replyCtx, lifetime, shutdown, metadata, payload); !errors.Is(err, response.ErrReply) || !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("NewSubscription() with a full reply queue error = %v, want %v and %v", err, response.ErrReply, context.DeadlineExceeded)
			}
			if !shutdown.IsRequested() {
				t.Error("shutdown before failed subscription confirmation was not requested, want requested")
			}
			stored, err := manager.consumerManager.GetConsumer("consumer-1")
			if tt.existing {
				if err != nil || stored.Info.Topic() != "old" || stored.Info.Offset() != 17 || stored.Info.Prefix() != "old/" || stored.Info.IPCPath() != "/old" {
					t.Errorf("metadata after failed confirmation = %+v, %v, want old, offset 17, prefix old/, queue /old and nil", stored, err)
				}
			} else if !errors.Is(err, consumer.ErrNotFound) {
				t.Errorf("GetConsumer() after failed first confirmation error = %v, want %v", err, consumer.ErrNotFound)
			}
			target.mu.Lock()
			attached := target.consumerMap.Len()
			target.mu.Unlock()
			if attached != 0 {
				t.Errorf("attached count after failed confirmation = %d, want 0", attached)
			}
			for i := 0; i < count; i++ {
				receiveTopicManagerReply(t, ctx, inbound, protocol.ServerShutdown)
			}
			if err := manager.NewSubscription(ctx, lifetime, lifecycle.GracefulShutdown{}, metadata, payload); err != nil {
				t.Errorf("NewSubscription() after draining the reply queue error = %v, want nil", err)
				return
			}
			receiveTopicManagerReply(t, ctx, inbound, protocol.TopicSubscriptionReply)
		})
	}
}

// TestManagerNewSubscriptionPropagatesSetupErrors checks cancellation and queue failures.
func TestManagerNewSubscriptionPropagatesSetupErrors(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	tests := []struct {
		name    string
		ipcPath string
		ctx     context.Context
		wantErr error
	}{
		{name: "invalid queue", ipcPath: "/invalid/queue", ctx: t.Context(), wantErr: mqueue.ErrInvalidName},
		{name: "missing queue", ipcPath: fmt.Sprintf("/subscription.missing.%d", os.Getpid()), ctx: t.Context(), wantErr: os.ErrNotExist},
		{name: "canceled lifetime", ipcPath: "/unused", ctx: canceled, wantErr: context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			subscriptionTarget(t, manager, "events")
			payload := *protocol.NewTopicSubscriptionPayload("consumer-1", "events", "", nil)
			if err := manager.NewSubscription(topicManagerContext(t), tt.ctx, lifecycle.GracefulShutdown{}, *protocol.NewClientMetadata(tt.ipcPath), payload); !errors.Is(err, tt.wantErr) {
				t.Errorf("NewSubscription() with %s error = %v, want %v", tt.name, err, tt.wantErr)
			}
			if got := manager.consumerManager.Len(); got != 0 {
				t.Errorf("consumer count after %s = %d, want 0", tt.name, got)
			}
		})
	}
}

// TestNewManagerInitializesState checks that both indexes are ready for use.
func TestNewManagerInitializesState(t *testing.T) {
	manager := NewManager()
	if topics, err := manager.topicMap.Values(); err != nil || len(topics) != 0 {
		t.Errorf("NewManager() topics = %v, error = %v, want no topics and nil error", topics, err)
	}
	if manager.consumerManager == nil {
		t.Error("NewManager() consumer manager = nil, want an initialized manager")
		return
	}
	if err := manager.consumerManager.AddConsumer("consumer-1", "events", 0, "", "/client"); err != nil {
		t.Errorf("AddConsumer(consumer-1, events, 0, empty prefix, /client) after NewManager() error = %v, want nil", err)
	}
}

// TestManagerListTopics checks the names returned for empty, minimal, and large registries.
func TestManagerListTopics(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "empty"},
		{name: "single topic", count: 1},
		{name: "many topics", count: 128},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			want := make([]string, tt.count)
			for i := range want {
				want[i] = fmt.Sprintf("topic-%03d", i)
				if err := manager.CreateTopic(ctx, ctx, metadata, *protocol.NewCreateTopicPayload(want[i])); err != nil {
					t.Fatalf("CreateTopic(%q) during setup error = %v, want nil", want[i], err)
				}
				receiveTopicManagerReply(t, ctx, inbound, protocol.CreateTopicReply)
			}
			if err := manager.ListTopics(ctx, metadata, *protocol.NewListTopicPayload()); err != nil {
				t.Errorf("ListTopics() with %d topics error = %v, want nil", tt.count, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ListTopicReply)
			reply, ok := data.Payload().(*protocol.ListTopicReplyPayload)
			if !ok {
				t.Errorf("ListTopics() with %d topics reply payload = %T, want *protocol.ListTopicReplyPayload", tt.count, data.Payload())
				return
			}
			got := reply.Topics()
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Errorf("ListTopics() with topics %q = %q, want the same names in any order", want, got)
			}
			if got := manager.topicMap.Len(); got != uint64(tt.count) {
				t.Errorf("topic count after ListTopics() with %d topics = %d, want %d", tt.count, got, tt.count)
			}
		})
	}
}

// TestManagerListTopicsPropagatesIndexErrors checks failures reading topic keys.
func TestManagerListTopicsPropagatesIndexErrors(t *testing.T) {
	tests := []struct {
		name  string
		index *hashmap.HashMap[id.ID, *Topic]
	}{
		{name: "nil index"},
		{name: "zero index", index: &hashmap.HashMap[id.ID, *Topic]{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			manager.topicMap = tt.index
			metadata := *protocol.NewClientMetadata("/unused")
			if err := manager.ListTopics(ctx, metadata, *protocol.NewListTopicPayload()); !errors.Is(err, hashmap.ErrUninitialized) {
				t.Errorf("ListTopics() with %s error = %v, want %v", tt.name, err, hashmap.ErrUninitialized)
			}
		})
	}
}

// TestManagerListTopicsPropagatesQueueErrors checks failures opening the client queue.
func TestManagerListTopicsPropagatesQueueErrors(t *testing.T) {
	tests := []struct {
		name    string
		ipcPath string
		wantErr error
	}{
		{name: "invalid queue name", ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "missing queue", ipcPath: fmt.Sprintf("/topic-manager.missing.%d", os.Getpid()), wantErr: os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			metadata := *protocol.NewClientMetadata(tt.ipcPath)
			if err := manager.ListTopics(ctx, metadata, *protocol.NewListTopicPayload()); !errors.Is(err, tt.wantErr) {
				t.Errorf("ListTopics() with queue %q error = %v, want %v", tt.ipcPath, err, tt.wantErr)
			}
		})
	}
}

// TestManagerListTopicsPropagatesStoppedContexts checks canceled and expired reply delivery.
func TestManagerListTopicsPropagatesStoppedContexts(t *testing.T) {
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
			manager := NewManager()
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			if err := manager.ListTopics(tt.ctx, metadata, *protocol.NewListTopicPayload()); !errors.Is(err, tt.wantErr) {
				t.Errorf("ListTopics() with a %s context error = %v, want %v", tt.name, err, tt.wantErr)
			}
		})
	}
}

// TestManagerListTopicsUsesEachRequestContext checks recovery after a canceled listing.
func TestManagerListTopicsUsesEachRequestContext(t *testing.T) {
	topicCtx := topicManagerContext(t)
	ctx, cancel := context.WithCancel(topicCtx)
	defer cancel()
	manager := NewManager()
	if err := manager.topicMap.Insert("events", NewTopic(topicCtx)); err != nil {
		t.Fatalf("Insert(events) during setup error = %v, want nil", err)
	}
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	cancel()
	if err := manager.ListTopics(ctx, metadata, *protocol.NewListTopicPayload()); !errors.Is(err, context.Canceled) {
		t.Errorf("ListTopics() with a canceled context error = %v, want %v", err, context.Canceled)
	}
	if err := manager.ListTopics(topicCtx, metadata, *protocol.NewListTopicPayload()); err != nil {
		t.Errorf("ListTopics() with a live context after a canceled listing error = %v, want nil", err)
		return
	}
	data := receiveTopicManagerReply(t, topicCtx, inbound, protocol.ListTopicReply)
	reply, ok := data.Payload().(*protocol.ListTopicReplyPayload)
	if !ok {
		t.Errorf("ListTopics() after a canceled listing reply payload = %T, want *protocol.ListTopicReplyPayload", data.Payload())
		return
	}
	if got := reply.Topics(); !slices.Equal(got, []string{"events"}) {
		t.Errorf("ListTopics() with [events] after a canceled listing = %q, want [events]", got)
	}
}

// TestManagerListTopicsPropagatesSendErrors checks replies exceeding the queue frame size.
func TestManagerListTopicsPropagatesSendErrors(t *testing.T) {
	ctx := topicManagerContext(t)
	manager := NewManager()
	for i := 0; i < 512; i++ {
		topicID := id.ID(fmt.Sprintf("topic-%03d", i))
		if err := manager.topicMap.Insert(topicID, NewTopic(ctx)); err != nil {
			t.Fatalf("Insert(%q) during setup error = %v, want nil", topicID, err)
		}
	}
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	if err := manager.ListTopics(ctx, metadata, *protocol.NewListTopicPayload()); !errors.Is(err, mqueue.ErrFrameTooLarge) {
		t.Errorf("ListTopics() with 512 topics error = %v, want %v", err, mqueue.ErrFrameTooLarge)
	}
}

// TestManagerListTopicsDuringCreation checks complete snapshots while topics are created.
func TestManagerListTopicsDuringCreation(t *testing.T) {
	ctx, cancel := context.WithCancel(topicManagerContext(t))
	manager := NewManager()
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	const count = 128
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	defer func() {
		cancel()
		workers.Wait()
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("topic-%03d", i)
			if err := manager.CreateTopic(ctx, ctx, metadata, *protocol.NewCreateTopicPayload(name)); err != nil {
				results <- fmt.Errorf("CreateTopic(%q): %w", name, err)
				return
			}
		}
		results <- nil
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < count; i++ {
			if err := manager.ListTopics(ctx, metadata, *protocol.NewListTopicPayload()); err != nil {
				results <- fmt.Errorf("ListTopics() request %d: %w", i, err)
				return
			}
		}
		results <- nil
	}()
	close(start)
	creations, listings := 0, 0
	for i := 0; i < 2*count; i++ {
		data, err := inbound.Recv(ctx)
		if err != nil {
			t.Errorf("Recv(%q) during %d creations and listings at reply %d error = %v, want nil", inbound.IPCPath(), count, i, err)
			break
		}
		switch reply := data.Payload().(type) {
		case *protocol.CreateTopicReplyPayload:
			creations++
			if data.Opcode() != protocol.CreateTopicReply {
				t.Errorf("concurrent CreateTopic() reply opcode = %v, want %v", data.Opcode(), protocol.CreateTopicReply)
			}
		case *protocol.ListTopicReplyPayload:
			listings++
			if data.Opcode() != protocol.ListTopicReply {
				t.Errorf("concurrent ListTopics() reply opcode = %v, want %v", data.Opcode(), protocol.ListTopicReply)
			}
			got := reply.Topics()
			if len(got) > count {
				t.Errorf("ListTopics() during %d creations returned %d topics, want at most %d", count, len(got), count)
				continue
			}
			want := make([]string, len(got))
			for j := range want {
				want[j] = fmt.Sprintf("topic-%03d", j)
			}
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Errorf("ListTopics() during sequential creation = %q, want a complete snapshot %q in any order", got, want)
			}
		default:
			t.Errorf("reply %d during concurrent creation and listing payload = %T, want creation or listing payload", i, data.Payload())
		}
	}
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("concurrent creation and listing error = %v, want nil", err)
		}
	}
	if creations != count || listings != count {
		t.Errorf("concurrent creation and listing received %d confirmations and %d lists, want %d each", creations, listings, count)
	}
}

// TestManagerCheckTopicConfirmsExistingTopics checks exact lookups without mutation.
func TestManagerCheckTopicConfirmsExistingTopics(t *testing.T) {
	tests := []struct {
		name  string
		count int
		topic string
	}{
		{name: "single topic", count: 1, topic: "topic-000"},
		{name: "many topics", count: 128, topic: "topic-127"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			for i := 0; i < tt.count; i++ {
				topicID := id.ID(fmt.Sprintf("topic-%03d", i))
				if err := manager.topicMap.Insert(topicID, NewTopic(ctx)); err != nil {
					t.Fatalf("Insert(%q) during setup error = %v, want nil", topicID, err)
				}
			}
			original, _ := manager.topicMap.Get(id.ID(tt.topic))
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			if err := manager.CheckTopic(ctx, metadata, *protocol.NewCheckTopicPayload(tt.topic)); err != nil {
				t.Errorf("CheckTopic(%q) with %d topics error = %v, want nil", tt.topic, tt.count, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.CheckTopicReply)
			if _, ok := data.Payload().(*protocol.CheckTopicReplyPayload); !ok {
				t.Errorf("CheckTopic(%q) payload = %T, want *protocol.CheckTopicReplyPayload", tt.topic, data.Payload())
			}
			if got := manager.topicMap.Len(); got != uint64(tt.count) {
				t.Errorf("topic count after CheckTopic(%q) = %d, want %d", tt.topic, got, tt.count)
			}
			if got, exists := manager.topicMap.Get(id.ID(tt.topic)); !exists || got != original {
				t.Errorf("Get(%q) after CheckTopic() = %p, %t, want %p and true", tt.topic, got, exists, original)
			}
		})
	}
}

// TestManagerCheckTopicRejectsMissingTopics checks empty and populated registries.
func TestManagerCheckTopicRejectsMissingTopics(t *testing.T) {
	tests := []struct {
		name  string
		count int
		topic string
	}{
		{name: "empty registry", topic: "topic-000"},
		{name: "single topic", count: 1, topic: "missing"},
		{name: "many topics", count: 128, topic: "missing"},
		{name: "prefix", count: 1, topic: "topic"},
		{name: "case sensitive", count: 1, topic: "Topic-000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			for i := 0; i < tt.count; i++ {
				topicID := id.ID(fmt.Sprintf("topic-%03d", i))
				if err := manager.topicMap.Insert(topicID, NewTopic(ctx)); err != nil {
					t.Fatalf("Insert(%q) during setup error = %v, want nil", topicID, err)
				}
			}
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			if err := manager.CheckTopic(ctx, metadata, *protocol.NewCheckTopicPayload(tt.topic)); err != nil {
				t.Errorf("CheckTopic(%q) error = %v, want nil after rejection", tt.topic, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ErrorReply)
			reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
			if !ok {
				t.Errorf("CheckTopic(%q) payload = %T, want *protocol.ErrorReplyPayload", tt.topic, data.Payload())
			} else if reply.Kind() != protocol.TopicNotFound || reply.Reason() == "" {
				t.Errorf("CheckTopic(%q) rejection = %v, %q, want %v and a nonempty reason", tt.topic, reply.Kind(), reply.Reason(), protocol.TopicNotFound)
			}
			if got := manager.topicMap.Len(); got != uint64(tt.count) {
				t.Errorf("topic count after CheckTopic(%q) = %d, want %d", tt.topic, got, tt.count)
			}
		})
	}
}

// TestManagerCheckTopicRejectsInvalidIDs checks validation before lookup.
func TestManagerCheckTopicRejectsInvalidIDs(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{name: "empty"},
		{name: "space", topic: "bad topic"},
		{name: "slash", topic: "bad/topic"},
		{name: "non-ASCII", topic: "événements"},
		{name: "too long", topic: strings.Repeat("a", 33)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			if err := manager.CheckTopic(ctx, metadata, *protocol.NewCheckTopicPayload(tt.topic)); err != nil {
				t.Errorf("CheckTopic(%q) error = %v, want nil after rejection", tt.topic, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ErrorReply)
			reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
			if !ok {
				t.Errorf("CheckTopic(%q) payload = %T, want *protocol.ErrorReplyPayload", tt.topic, data.Payload())
			} else if reply.Kind() != protocol.InvalidRequest || reply.Reason() == "" {
				t.Errorf("CheckTopic(%q) rejection = %v, %q, want %v and a nonempty reason", tt.topic, reply.Kind(), reply.Reason(), protocol.InvalidRequest)
			}
			if got := manager.topicMap.Len(); got != 0 {
				t.Errorf("topic count after CheckTopic(%q) = %d, want 0", tt.topic, got)
			}
		})
	}
}

// TestManagerCheckTopicPropagatesQueueErrors checks failed reply connections.
func TestManagerCheckTopicPropagatesQueueErrors(t *testing.T) {
	tests := []struct {
		name    string
		ipcPath string
		wantErr error
	}{
		{name: "invalid queue name", ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "missing queue", ipcPath: fmt.Sprintf("/topic-check.missing.%d", os.Getpid()), wantErr: os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			metadata := *protocol.NewClientMetadata(tt.ipcPath)
			if err := manager.CheckTopic(topicManagerContext(t), metadata, *protocol.NewCheckTopicPayload("events")); !errors.Is(err, tt.wantErr) {
				t.Errorf("CheckTopic(events) with queue %q error = %v, want %v", tt.ipcPath, err, tt.wantErr)
			}
		})
	}
}

// TestManagerCheckTopicPropagatesStoppedContexts checks canceled reply delivery.
func TestManagerCheckTopicPropagatesStoppedContexts(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, expireCancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer expireCancel()
	tests := []struct {
		name    string
		ctx     context.Context
		topic   string
		wantErr error
	}{
		{name: "canceled confirmation", ctx: canceled, topic: "events", wantErr: context.Canceled},
		{name: "expired confirmation", ctx: expired, topic: "events", wantErr: context.DeadlineExceeded},
		{name: "canceled missing topic", ctx: canceled, topic: "missing", wantErr: context.Canceled},
		{name: "expired missing topic", ctx: expired, topic: "missing", wantErr: context.DeadlineExceeded},
		{name: "canceled invalid topic", ctx: canceled, topic: "bad/topic", wantErr: context.Canceled},
		{name: "expired invalid topic", ctx: expired, topic: "bad/topic", wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			if err := manager.topicMap.Insert("events", NewTopic(t.Context())); err != nil {
				t.Fatalf("Insert(events) during setup error = %v, want nil", err)
			}
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			if err := manager.CheckTopic(tt.ctx, metadata, *protocol.NewCheckTopicPayload(tt.topic)); !errors.Is(err, tt.wantErr) {
				t.Errorf("CheckTopic(%q) with %s error = %v, want %v", tt.topic, tt.name, err, tt.wantErr)
			}
		})
	}
}

// TestManagerCheckTopicDuringCreation checks lookups alongside registry growth.
func TestManagerCheckTopicDuringCreation(t *testing.T) {
	ctx, cancel := context.WithCancel(topicManagerContext(t))
	manager := NewManager()
	if err := manager.topicMap.Insert("events", NewTopic(ctx)); err != nil {
		cancel()
		t.Fatalf("Insert(events) during setup error = %v, want nil", err)
	}
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	const count = 128
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	defer func() {
		cancel()
		workers.Wait()
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("topic-%03d", i)
			if err := manager.CreateTopic(ctx, ctx, metadata, *protocol.NewCreateTopicPayload(name)); err != nil {
				results <- fmt.Errorf("CreateTopic(%q): %w", name, err)
				return
			}
		}
		results <- nil
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < count; i++ {
			if err := manager.CheckTopic(ctx, metadata, *protocol.NewCheckTopicPayload("events")); err != nil {
				results <- fmt.Errorf("CheckTopic(events) request %d: %w", i, err)
				return
			}
		}
		results <- nil
	}()
	close(start)
	creations, checks := 0, 0
	for i := 0; i < 2*count; i++ {
		data, err := inbound.Recv(ctx)
		if err != nil {
			t.Errorf("Recv(%q) during creation and checks at reply %d error = %v, want nil", inbound.IPCPath(), i, err)
			cancel()
			break
		}
		switch data.Payload().(type) {
		case *protocol.CreateTopicReplyPayload:
			creations++
		case *protocol.CheckTopicReplyPayload:
			checks++
		default:
			t.Errorf("reply %d during creation and checks payload = %T, want creation or check payload", i, data.Payload())
		}
	}
	workers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("concurrent creation and checks error = %v, want nil", err)
		}
	}
	if creations != count || checks != count {
		t.Errorf("concurrent creation and checks received %d creations and %d checks, want %d each", creations, checks, count)
	}
}

// TestManagerProduceMessageForwardsToTopic checks stored routing and success replies.
func TestManagerProduceMessageForwardsToTopic(t *testing.T) {
	tests := []struct {
		name  string
		count int
		key   []byte
		value []byte
	}{
		{name: "empty message", count: 1},
		{name: "binary message", count: 1, key: []byte{0, 255}, value: []byte{255, 0}},
		{name: "size limit", count: 1, key: []byte("k"), value: make([]byte, protocol.MessageSize-1)},
		{name: "many topics", count: 128, key: []byte("key"), value: []byte("body")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			manager := NewManager()
			topics := make([]*Topic, 0, tt.count)
			t.Cleanup(func() {
				cancel()
				for _, topic := range topics {
					waitTopicLoop(t, topic)
				}
			})
			for i := 0; i < tt.count; i++ {
				target := NewTopic(ctx)
				topics = append(topics, target)
				topicID := id.ID(fmt.Sprintf("topic-%03d", i))
				if err := manager.topicMap.Insert(topicID, target); err != nil {
					t.Errorf("Insert(%q) during setup error = %v, want nil", topicID, err)
					return
				}
			}
			topicName := fmt.Sprintf("topic-%03d", tt.count-1)
			payload := *protocol.NewProduceMessagePayload(topicName, protocol.NewMessage(tt.key, tt.value))
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			if err := manager.ProduceMessage(ctx, lifecycle.GracefulShutdown{}, metadata, payload); err != nil {
				t.Errorf("ProduceMessage(%q) error = %v, want nil after confirmation", topicName, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ProduceMessageReply)
			if _, ok := data.Payload().(*protocol.ProduceMessageReplyPayload); !ok {
				t.Errorf("ProduceMessage(%q) reply payload = %T, want *protocol.ProduceMessageReplyPayload", topicName, data.Payload())
			}
			cancel()
			for i, target := range topics {
				if !waitTopicLoop(t, target) {
					return
				}
				wantCount := 0
				if i == tt.count-1 {
					wantCount = 1
				}
				if got := len(target.messages); got != wantCount {
					t.Errorf("stored count on topic-%03d after ProduceMessage(%q) = %d, want %d", i, topicName, got, wantCount)
				}
				if wantCount == 1 && len(target.messages) == 1 {
					message := target.messages[0]
					if !bytes.Equal(message.Key(), tt.key) || !bytes.Equal(message.Value(), tt.value) {
						t.Errorf("ProduceMessage(%q) stored key %x, value %x, want key %x, value %x", topicName, message.Key(), message.Value(), tt.key, tt.value)
					}
				}
			}
		})
	}
}

// TestManagerProduceMessageRejectsRequests checks invalid and missing topics.
func TestManagerProduceMessageRejectsRequests(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		count    int
		wantKind protocol.ErrorKind
	}{
		{name: "empty name", wantKind: protocol.InvalidRequest},
		{name: "invalid name", topic: "bad/topic", wantKind: protocol.InvalidRequest},
		{name: "long name", topic: strings.Repeat("a", 33), wantKind: protocol.InvalidRequest},
		{name: "empty registry", topic: "missing", wantKind: protocol.TopicNotFound},
		{name: "populated registry", topic: "missing", count: 128, wantKind: protocol.TopicNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			for i := 0; i < tt.count; i++ {
				topicID := id.ID(fmt.Sprintf("topic-%03d", i))
				if err := manager.topicMap.Insert(topicID, NewTopic(ctx)); err != nil {
					t.Fatalf("Insert(%q) during setup error = %v, want nil", topicID, err)
				}
			}
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			payload := *protocol.NewProduceMessagePayload(tt.topic, protocol.NewMessage(nil, nil))
			if err := manager.ProduceMessage(ctx, lifecycle.GracefulShutdown{}, metadata, payload); err != nil {
				t.Errorf("ProduceMessage(%q) error = %v, want nil after rejection", tt.topic, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ErrorReply)
			reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
			if !ok {
				t.Errorf("ProduceMessage(%q) payload = %T, want *protocol.ErrorReplyPayload", tt.topic, data.Payload())
			} else if reply.Kind() != tt.wantKind || reply.Reason() == "" {
				t.Errorf("ProduceMessage(%q) rejection = %v, %q, want %v and a nonempty reason", tt.topic, reply.Kind(), reply.Reason(), tt.wantKind)
			}
			if got := manager.topicMap.Len(); got != uint64(tt.count) {
				t.Errorf("topic count after ProduceMessage(%q) = %d, want %d", tt.topic, got, tt.count)
			}
			topics, err := manager.topicMap.Values()
			if err != nil {
				t.Errorf("Values() after rejected ProduceMessage(%q) error = %v, want nil", tt.topic, err)
				return
			}
			for _, topic := range topics {
				topic.mu.Lock()
				stored := len(topic.messages)
				topic.mu.Unlock()
				if stored != 0 {
					t.Errorf("stored count after rejected ProduceMessage(%q) = %d, want 0", tt.topic, stored)
				}
			}
		})
	}
}

// TestManagerProduceMessageRejectsShutdown checks shutdown takes priority over topic lookup.
func TestManagerProduceMessageRejectsShutdown(t *testing.T) {
	tests := []struct {
		name       string
		topic      string
		registered bool
	}{
		{name: "existing topic", topic: "events", registered: true},
		{name: "missing topic", topic: "missing", registered: true},
		{name: "invalid topic", topic: "bad/topic", registered: true},
		{name: "empty registry", topic: "events"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			target, _, _ := newTestTopic(t)
			var wantCount uint64
			if tt.registered {
				if err := manager.topicMap.Insert("events", target); err != nil {
					t.Errorf("Insert(events) during setup error = %v, want nil", err)
					return
				}
				wantCount = 1
			}
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			payload := *protocol.NewProduceMessagePayload(tt.topic, protocol.NewMessage(nil, nil))
			requested := make(chan struct{})
			shutdown := lifecycle.NewGracefulShutdown(requested)
			close(requested)
			if err := manager.ProduceMessage(ctx, shutdown, metadata, payload); err != nil {
				t.Errorf("ProduceMessage(%q) during shutdown error = %v, want nil after rejection", tt.topic, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ServerShutdown)
			if _, ok := data.Payload().(*protocol.ServerShutdownPayload); !ok {
				t.Errorf("ProduceMessage(%q) during shutdown payload = %T, want *protocol.ServerShutdownPayload", tt.topic, data.Payload())
			}
			if got := manager.topicMap.Len(); got != wantCount {
				t.Errorf("topic count after ProduceMessage(%q) during shutdown = %d, want %d", tt.topic, got, wantCount)
			}
			target.mu.Lock()
			stored := len(target.messages)
			target.mu.Unlock()
			if stored != 0 {
				t.Errorf("stored count after ProduceMessage(%q) during shutdown = %d, want 0", tt.topic, stored)
			}
		})
	}
}

// TestManagerProduceMessageFinishesAfterAdmission checks a shutdown during handoff.
func TestManagerProduceMessageFinishesAfterAdmission(t *testing.T) {
	manager := NewManager()
	target, _, stop := newTestTopic(t)
	if err := manager.topicMap.Insert("events", target); err != nil {
		t.Errorf("Insert(events) during setup error = %v, want nil", err)
		return
	}
	requested := make(chan struct{})
	shutdown := lifecycle.NewGracefulShutdown(requested)
	ctx := &produceSelectContext{Context: topicManagerContext(t), selected: requested}
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	payload := *protocol.NewProduceMessagePayload("events", protocol.NewMessage([]byte("key"), []byte("body")))
	if err := manager.ProduceMessage(ctx, shutdown, metadata, payload); err != nil {
		t.Errorf("ProduceMessage(events) admitted before shutdown error = %v, want nil after confirmation", err)
		return
	}
	data := receiveTopicManagerReply(t, ctx, inbound, protocol.ProduceMessageReply)
	if _, ok := data.Payload().(*protocol.ProduceMessageReplyPayload); !ok {
		t.Errorf("ProduceMessage(events) admitted before shutdown reply payload = %T, want *protocol.ProduceMessageReplyPayload", data.Payload())
	}
	if !shutdown.IsRequested() {
		t.Error("shutdown during admitted production was not requested, want requested")
	}
	stop()
	if !waitTopicLoop(t, target) {
		return
	}
	if len(target.messages) != 1 {
		t.Errorf("stored count after admitted production during shutdown = %d, want 1", len(target.messages))
	} else if !bytes.Equal(target.messages[0].Key(), []byte("key")) || !bytes.Equal(target.messages[0].Value(), []byte("body")) {
		t.Errorf("stored message after admitted production during shutdown = key %x, value %x, want key %x, value %x", target.messages[0].Key(), target.messages[0].Value(), []byte("key"), []byte("body"))
	}
}

// TestManagerProduceMessagePropagatesTopicCancellation checks stopped sends.
func TestManagerProduceMessagePropagatesTopicCancellation(t *testing.T) {
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
			manager := NewManager()
			if err := manager.topicMap.Insert("events", NewTopic(t.Context())); err != nil {
				t.Fatalf("Insert(events) during setup error = %v, want nil", err)
			}
			metadata := *protocol.NewClientMetadata("/unused")
			payload := *protocol.NewProduceMessagePayload("events", protocol.NewMessage(nil, nil))
			if err := manager.ProduceMessage(tt.ctx, lifecycle.GracefulShutdown{}, metadata, payload); !errors.Is(err, tt.wantErr) {
				t.Errorf("ProduceMessage(events) with %s context error = %v, want %v", tt.name, err, tt.wantErr)
			}
		})
	}
}

// TestManagerProduceMessagePropagatesConfirmationQueueErrors checks replies after acceptance.
func TestManagerProduceMessagePropagatesConfirmationQueueErrors(t *testing.T) {
	tests := []struct {
		name    string
		ipcPath string
		wantErr error
	}{
		{name: "invalid queue name", ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "missing queue", ipcPath: fmt.Sprintf("/produce.missing.%d", os.Getpid()), wantErr: os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			target, _, stop := newTestTopic(t)
			if err := manager.topicMap.Insert("events", target); err != nil {
				t.Errorf("Insert(events) during setup error = %v, want nil", err)
				return
			}
			metadata := *protocol.NewClientMetadata(tt.ipcPath)
			payload := *protocol.NewProduceMessagePayload("events", protocol.NewMessage(nil, nil))
			if err := manager.ProduceMessage(ctx, lifecycle.GracefulShutdown{}, metadata, payload); !errors.Is(err, tt.wantErr) {
				t.Errorf("ProduceMessage(events) with reply queue %q error = %v, want %v", tt.ipcPath, err, tt.wantErr)
			}
			stop()
			if !waitTopicLoop(t, target) {
				return
			}
			if got := len(target.messages); got != 1 {
				t.Errorf("stored count after ProduceMessage(events) with reply queue %q = %d, want 1", tt.ipcPath, got)
			}
		})
	}
}

// TestManagerProduceMessagePropagatesQueueErrors checks failed rejection replies.
func TestManagerProduceMessagePropagatesQueueErrors(t *testing.T) {
	tests := []struct {
		name    string
		ipcPath string
		wantErr error
	}{
		{name: "invalid queue name", ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "missing queue", ipcPath: fmt.Sprintf("/produce.missing.%d", os.Getpid()), wantErr: os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewManager()
			metadata := *protocol.NewClientMetadata(tt.ipcPath)
			payload := *protocol.NewProduceMessagePayload("missing", protocol.NewMessage(nil, nil))
			if err := manager.ProduceMessage(topicManagerContext(t), lifecycle.GracefulShutdown{}, metadata, payload); !errors.Is(err, tt.wantErr) {
				t.Errorf("ProduceMessage(missing) with reply queue %q error = %v, want %v", tt.ipcPath, err, tt.wantErr)
			}
		})
	}
}

// TestManagerCreateTopics checks registration and confirmation for increasing topic counts.
func TestManagerCreateTopics(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "empty"},
		{name: "single topic", count: 1},
		{name: "many topics", count: 128},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			registered := make(map[*Topic]id.ID)
			for i := 0; i < tt.count; i++ {
				topicID := id.ID(fmt.Sprintf("topic-%d", i))
				if err := manager.CreateTopic(ctx, ctx, metadata, *protocol.NewCreateTopicPayload(string(topicID))); err != nil {
					t.Errorf("CreateTopic(%q) error = %v, want nil", topicID, err)
					return
				}
				data := receiveTopicManagerReply(t, ctx, inbound, protocol.CreateTopicReply)
				if _, ok := data.Payload().(*protocol.CreateTopicReplyPayload); !ok {
					t.Errorf("CreateTopic(%q) reply payload = %T, want *protocol.CreateTopicReplyPayload", topicID, data.Payload())
				}
				stored, exists := manager.topicMap.Get(topicID)
				if !exists || stored == nil {
					t.Errorf("Get(%q) after CreateTopic() = %p, exists = %t, want an initialized topic and true", topicID, stored, exists)
					continue
				}
				if err := stored.validate(); err != nil {
					t.Errorf("Validate() for created topic %q error = %v, want nil", topicID, err)
				}
				if previous, reused := registered[stored]; reused {
					t.Errorf("CreateTopic(%q) reused topic %p registered for %q, want a distinct topic", topicID, stored, previous)
				}
				registered[stored] = topicID
			}
			if got := manager.topicMap.Len(); got != uint64(tt.count) {
				t.Errorf("topic count after %d creations = %d, want %d", tt.count, got, tt.count)
			}
		})
	}
}

// TestManagerCreateTopicRejectsInvalidIDs checks rejection without changing stored topics.
func TestManagerCreateTopicRejectsInvalidIDs(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{name: "empty"},
		{name: "space", topic: "bad topic"},
		{name: "slash", topic: "bad/topic"},
		{name: "non-ASCII", topic: "événements"},
		{name: "too long", topic: strings.Repeat("a", 33)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			original := NewTopic(ctx)
			if err := manager.topicMap.Insert("events", original); err != nil {
				t.Fatalf("Insert(events) during setup error = %v, want nil", err)
			}
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			if err := manager.CreateTopic(ctx, ctx, metadata, *protocol.NewCreateTopicPayload(tt.topic)); err != nil {
				t.Errorf("CreateTopic(%q) error = %v, want nil after sending rejection", tt.topic, err)
				return
			}
			data := receiveTopicManagerReply(t, ctx, inbound, protocol.ErrorReply)
			reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
			if !ok {
				t.Errorf("CreateTopic(%q) reply payload = %T, want *protocol.ErrorReplyPayload", tt.topic, data.Payload())
			} else if reply.Kind() != protocol.InvalidRequest || reply.Reason() == "" {
				t.Errorf("CreateTopic(%q) rejection = %v, %q, want %v and a nonempty reason", tt.topic, reply.Kind(), reply.Reason(), protocol.InvalidRequest)
			}
			if got := manager.topicMap.Len(); got != 1 {
				t.Errorf("topic count after rejecting %q = %d, want 1", tt.topic, got)
			}
			if stored, exists := manager.topicMap.Get("events"); !exists || stored != original {
				t.Errorf("Get(events) after rejecting %q = %p, exists = %t, want %p and true", tt.topic, stored, exists, original)
			}
		})
	}
}

// TestManagerCreateTopicRejectsDuplicates checks that a duplicate preserves the original topic.
func TestManagerCreateTopicRejectsDuplicates(t *testing.T) {
	ctx := topicManagerContext(t)
	manager := NewManager()
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	payload := *protocol.NewCreateTopicPayload("events")
	if err := manager.CreateTopic(ctx, ctx, metadata, payload); err != nil {
		t.Fatalf("CreateTopic(events) during setup error = %v, want nil", err)
	}
	receiveTopicManagerReply(t, ctx, inbound, protocol.CreateTopicReply)
	original, exists := manager.topicMap.Get("events")
	if !exists || original == nil {
		t.Fatalf("Get(events) during setup = %p, exists = %t, want an initialized topic and true", original, exists)
	}
	if err := manager.CreateTopic(ctx, ctx, metadata, payload); err != nil {
		t.Errorf("CreateTopic(events) on a duplicate error = %v, want nil after sending rejection", err)
		return
	}
	data := receiveTopicManagerReply(t, ctx, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok {
		t.Errorf("duplicate CreateTopic(events) reply payload = %T, want *protocol.ErrorReplyPayload", data.Payload())
	} else if reply.Kind() != protocol.TopicExists || reply.Reason() == "" {
		t.Errorf("duplicate CreateTopic(events) rejection = %v, %q, want %v and a nonempty reason", reply.Kind(), reply.Reason(), protocol.TopicExists)
	}
	if stored, exists := manager.topicMap.Get("events"); !exists || stored != original {
		t.Errorf("Get(events) after duplicate creation = %p, exists = %t, want %p and true", stored, exists, original)
	}
	if got := manager.topicMap.Len(); got != 1 {
		t.Errorf("topic count after duplicate CreateTopic(events) = %d, want 1", got)
	}
}

// TestManagerCreateTopicPropagatesIndexErrors checks that insertion failures reach the caller.
func TestManagerCreateTopicPropagatesIndexErrors(t *testing.T) {
	ctx := topicManagerContext(t)
	manager := NewManager()
	manager.topicMap = nil
	metadata := *protocol.NewClientMetadata("/unused")
	if err := manager.CreateTopic(ctx, ctx, metadata, *protocol.NewCreateTopicPayload("events")); !errors.Is(err, hashmap.ErrUninitialized) {
		t.Errorf("CreateTopic(events) with an uninitialized index error = %v, want %v", err, hashmap.ErrUninitialized)
	}
}

// TestManagerCreateTopicPropagatesQueueErrors checks failures opening the client queue.
func TestManagerCreateTopicPropagatesQueueErrors(t *testing.T) {
	tests := []struct {
		name    string
		ipcPath string
		wantErr error
	}{
		{name: "invalid queue name", ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "missing queue", ipcPath: fmt.Sprintf("/topic-manager.missing.%d", os.Getpid()), wantErr: os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := topicManagerContext(t)
			manager := NewManager()
			metadata := *protocol.NewClientMetadata(tt.ipcPath)
			if err := manager.CreateTopic(ctx, ctx, metadata, *protocol.NewCreateTopicPayload("events")); !errors.Is(err, tt.wantErr) {
				t.Errorf("CreateTopic(events) with queue %q error = %v, want %v", tt.ipcPath, err, tt.wantErr)
			}
		})
	}
}

// TestManagerCreateTopicPropagatesStoppedContexts checks canceled and expired reply delivery.
func TestManagerCreateTopicPropagatesStoppedContexts(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, expireCancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer expireCancel()
	tests := []struct {
		name      string
		ctx       context.Context
		topic     string
		wantErr   error
		wantCount uint64
	}{
		{name: "canceled creation", ctx: canceled, topic: "orders", wantErr: context.Canceled, wantCount: 2},
		{name: "expired creation", ctx: expired, topic: "orders", wantErr: context.DeadlineExceeded, wantCount: 2},
		{name: "canceled duplicate rejection", ctx: canceled, topic: "events", wantErr: context.Canceled, wantCount: 1},
		{name: "expired duplicate rejection", ctx: expired, topic: "events", wantErr: context.DeadlineExceeded, wantCount: 1},
		{name: "canceled invalid ID rejection", ctx: canceled, topic: "bad/topic", wantErr: context.Canceled, wantCount: 1},
		{name: "expired invalid ID rejection", ctx: expired, topic: "bad/topic", wantErr: context.DeadlineExceeded, wantCount: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topicCtx := topicManagerContext(t)
			manager := NewManager()
			original := NewTopic(topicCtx)
			if err := manager.topicMap.Insert("events", original); err != nil {
				t.Fatalf("Insert(events) during setup error = %v, want nil", err)
			}
			inbound := newTopicManagerInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			if err := manager.CreateTopic(tt.ctx, topicCtx, metadata, *protocol.NewCreateTopicPayload(tt.topic)); !errors.Is(err, tt.wantErr) {
				t.Errorf("CreateTopic(%q) with %s error = %v, want %v", tt.topic, tt.name, err, tt.wantErr)
			}
			if got := manager.topicMap.Len(); got != tt.wantCount {
				t.Errorf("topic count after CreateTopic(%q) with %s = %d, want %d", tt.topic, tt.name, got, tt.wantCount)
			}
			if stored, exists := manager.topicMap.Get("events"); !exists || stored != original {
				t.Errorf("Get(events) after CreateTopic(%q) with %s = %p, exists = %t, want %p and true", tt.topic, tt.name, stored, exists, original)
			}
		})
	}
}

// TestManagerCreateTopicUsesEachRequestContext checks recovery after a canceled reply.
func TestManagerCreateTopicUsesEachRequestContext(t *testing.T) {
	topicCtx := topicManagerContext(t)
	ctx, cancel := context.WithCancel(topicCtx)
	defer cancel()
	manager := NewManager()
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	cancel()
	if err := manager.CreateTopic(ctx, topicCtx, metadata, *protocol.NewCreateTopicPayload("events")); !errors.Is(err, context.Canceled) {
		t.Errorf("CreateTopic(events) with a canceled context error = %v, want %v", err, context.Canceled)
	}
	if err := manager.CreateTopic(topicCtx, topicCtx, metadata, *protocol.NewCreateTopicPayload("orders")); err != nil {
		t.Errorf("CreateTopic(orders) with a live context after a canceled request error = %v, want nil", err)
		return
	}
	data := receiveTopicManagerReply(t, topicCtx, inbound, protocol.CreateTopicReply)
	if _, ok := data.Payload().(*protocol.CreateTopicReplyPayload); !ok {
		t.Errorf("CreateTopic(orders) after a canceled request reply payload = %T, want *protocol.CreateTopicReplyPayload", data.Payload())
	}
}

// createTopicsConcurrently starts a bounded batch and collects replies after all calls return.
func createTopicsConcurrently(t *testing.T, topicIDs []id.ID) (*Manager, []protocol.Data) {
	t.Helper()
	ctx := topicManagerContext(t)
	manager := NewManager()
	inbound := newTopicManagerInbox(t)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	start := make(chan struct{})
	results := make(chan error, len(topicIDs))
	var workers sync.WaitGroup
	// The batch stays below the queue capacity so sends can finish before receiving.
	for _, topicID := range topicIDs {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			if err := manager.CreateTopic(ctx, ctx, metadata, *protocol.NewCreateTopicPayload(string(topicID))); err != nil {
				results <- fmt.Errorf("CreateTopic(%q): %w", topicID, err)
			}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	failed := false
	for err := range results {
		t.Errorf("concurrent creation error = %v, want nil", err)
		failed = true
	}
	if failed {
		return nil, nil
	}
	replies := make([]protocol.Data, len(topicIDs))
	for i := range replies {
		data, err := inbound.Recv(ctx)
		if err != nil {
			t.Errorf("Recv(%q) for topics %q reply %d error = %v, want nil", inbound.IPCPath(), topicIDs, i, err)
			return nil, nil
		}
		replies[i] = data
	}
	return manager, replies
}

// TestManagerCreateTopicsConcurrently checks registration of distinct concurrent requests.
func TestManagerCreateTopicsConcurrently(t *testing.T) {
	topicIDs := make([]id.ID, 8)
	for i := range topicIDs {
		topicIDs[i] = id.ID(fmt.Sprintf("topic-%d", i))
	}
	manager, replies := createTopicsConcurrently(t, topicIDs)
	if manager == nil {
		return
	}
	for i, data := range replies {
		if _, ok := data.Payload().(*protocol.CreateTopicReplyPayload); !ok || data.Opcode() != protocol.CreateTopicReply {
			t.Errorf("concurrent creation reply %d = opcode %v, payload %T, want %v and *protocol.CreateTopicReplyPayload", i, data.Opcode(), data.Payload(), protocol.CreateTopicReply)
		}
	}
	if got := manager.topicMap.Len(); got != uint64(len(topicIDs)) {
		t.Errorf("topic count after concurrent creation of %q = %d, want %d", topicIDs, got, len(topicIDs))
	}
	for _, topicID := range topicIDs {
		if stored, exists := manager.topicMap.Get(topicID); !exists || stored == nil {
			t.Errorf("Get(%q) after concurrent creation = %p, exists = %t, want an initialized topic and true", topicID, stored, exists)
		}
	}
}

// TestManagerCreateTopicRejectsConcurrentDuplicates checks that one request wins for a shared ID.
func TestManagerCreateTopicRejectsConcurrentDuplicates(t *testing.T) {
	topicIDs := make([]id.ID, 8)
	for i := range topicIDs {
		topicIDs[i] = "events"
	}
	manager, replies := createTopicsConcurrently(t, topicIDs)
	if manager == nil {
		return
	}
	successes, duplicates := 0, 0
	for i, data := range replies {
		switch reply := data.Payload().(type) {
		case *protocol.CreateTopicReplyPayload:
			successes++
		case *protocol.ErrorReplyPayload:
			if reply.Kind() != protocol.TopicExists {
				t.Errorf("concurrent CreateTopic(events) reply %d rejection = %v, want %v", i, reply.Kind(), protocol.TopicExists)
			}
			duplicates++
		default:
			t.Errorf("concurrent CreateTopic(events) reply %d payload = %T, want creation or rejection payload", i, data.Payload())
		}
	}
	if successes != 1 || duplicates != len(topicIDs)-1 {
		t.Errorf("concurrent CreateTopic(events) responses = %d successes, %d duplicates, want 1 and %d", successes, duplicates, len(topicIDs)-1)
	}
	if got := manager.topicMap.Len(); got != 1 {
		t.Errorf("topic count after concurrent CreateTopic(events) = %d, want 1", got)
	}
	if stored, exists := manager.topicMap.Get("events"); !exists || stored == nil {
		t.Errorf("Get(events) after concurrent creation = %p, exists = %t, want an initialized topic and true", stored, exists)
	}
}
