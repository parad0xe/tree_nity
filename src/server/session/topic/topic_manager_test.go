package topic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/server/utils/hashmap"
)

// topicManagerContext bounds IPC operations and cancels them when the test ends.
func topicManagerContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
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

// TestManagerReplyRejectsInvalidPayloads checks validation before opening a client queue.
func TestManagerReplyRejectsInvalidPayloads(t *testing.T) {
	ctx := topicManagerContext(t)
	manager := NewManager()
	metadata := *protocol.NewClientMetadata("/missing")
	if err := manager.reply(ctx, metadata, protocol.CreateTopicReply, nil); !errors.Is(err, protocol.ErrMissingPayload) {
		t.Errorf("reply(CreateTopicReply, nil) error = %v, want %v", err, protocol.ErrMissingPayload)
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
