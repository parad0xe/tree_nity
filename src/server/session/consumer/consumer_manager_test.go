package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/server/utils/hashmap"
)

func newTestManager(t *testing.T, infos ...Info) *Manager {
	t.Helper()
	manager := NewManager()
	if manager == nil {
		t.Errorf("NewManager() = nil, want an initialized manager")
		return nil
	}
	for _, info := range infos {
		if err := manager.AddConsumer(info.id, info.topic, info.offset, info.prefix, info.ipcPath); err != nil {
			t.Errorf("AddConsumer(%q, %q, %d, %q, %q) during setup error = %v, want nil", info.id, info.topic, info.offset, info.prefix, info.ipcPath, err)
			return nil
		}
	}
	return manager
}

func openTestConsumer(t *testing.T, manager *Manager, consumerID id.ID) *Consumer {
	t.Helper()
	c, err := manager.OpenConsumer(consumerID)
	if err != nil || c == nil || c.Connection == nil {
		t.Errorf("OpenConsumer(%q) = %+v, error = %v, want a consumer with a connection and nil error", consumerID, c, err)
		return nil
	}
	cleanupTestConnection(t, c.Connection)
	return c
}

// TestManagerOpenConsumerPropagatesConnectionError checks queue errors and metadata ownership.
func TestManagerOpenConsumerPropagatesConnectionError(t *testing.T) {
	tests := []struct {
		name    string
		ipcPath string
		wantErr error
	}{
		{name: "invalid name", ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "missing queue", ipcPath: fmt.Sprintf("/consumer.%s.%d", t.Name(), os.Getpid()), wantErr: os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: tt.ipcPath}
			manager := newTestManager(t, original)
			if manager == nil {
				return
			}
			c, err := manager.OpenConsumer(original.id)
			if c != nil {
				cleanupTestConnection(t, c.Connection)
				t.Errorf("OpenConsumer(%q) with queue %q = %+v, want nil on connection initialization failure", original.id, tt.ipcPath, c)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("OpenConsumer(%q) with queue %q error = %v, want %v", original.id, tt.ipcPath, err, tt.wantErr)
			}
			stored, exists := manager.consumerTable.Get(original.id)
			if !exists || stored == nil || *stored != original {
				t.Errorf("stored metadata for %q after failed OpenConsumer with queue %q = %+v, exists = %t, want %+v and true", original.id, tt.ipcPath, stored, exists, original)
			}
		})
	}
}

// TestManagerUninitialized checks all public operations on missing manager state.
func TestManagerUninitialized(t *testing.T) {
	tests := []struct {
		name    string
		manager *Manager
	}{
		{name: "nil receiver"},
		{name: "zero value", manager: &Manager{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumerID := id.ID("consumer-1")
			if err := tt.manager.AddConsumer(consumerID, "events", 7, "events/", "/consumer-1"); !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("AddConsumer(%q, %q, 7, %q, %q) on %s error = %v, want %v", consumerID, "events", "events/", "/consumer-1", tt.name, err, ErrManagerUninitialized)
			}
			if c, err := tt.manager.OpenConsumer(consumerID); c != nil || !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("OpenConsumer(%q) on %s = %+v, error = %v, want nil and %v", consumerID, tt.name, c, err, ErrManagerUninitialized)
			}
			if err := tt.manager.UpdateOffset(consumerID, 8); !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("UpdateOffset(%q, 8) on %s error = %v, want %v", consumerID, tt.name, err, ErrManagerUninitialized)
			}
			if err := tt.manager.UpdateTopic(consumerID, "other-topic"); !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("UpdateTopic(%q, %q) on %s error = %v, want %v", consumerID, "other-topic", tt.name, err, ErrManagerUninitialized)
			}
			if err := tt.manager.RemoveConsumer(consumerID); !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("RemoveConsumer(%q) on %s error = %v, want %v", consumerID, tt.name, err, ErrManagerUninitialized)
			}
			if got := tt.manager.Len(); got != 0 {
				t.Errorf("Len() on %s = %d, want 0", tt.name, got)
			}
		})
	}
}

// TestManagerLifecycle checks empty, minimal, and large indexes through removal.
func TestManagerLifecycle(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "empty"},
		{name: "single consumer", count: 1},
		{name: "many consumers", count: 512},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newTestManager(t)
			if manager == nil {
				return
			}
			if got := manager.Len(); got != 0 {
				t.Errorf("Len() on a new manager with target size %d = %d, want 0", tt.count, got)
			}
			for i := 0; i < tt.count; i++ {
				consumerID := id.ID(fmt.Sprintf("consumer-%d", i))
				if err := manager.AddConsumer(consumerID, "events", uint32(i), "events/", "/consumer-tests"); err != nil {
					t.Errorf("AddConsumer(%q, %q, %d, %q, %q) of %d consumers error = %v, want nil", consumerID, "events", i, "events/", "/consumer-tests", tt.count, err)
					return
				}
			}
			if got := manager.Len(); got != uint64(tt.count) {
				t.Errorf("Len() after adding %d consumers = %d, want %d", tt.count, got, tt.count)
			}
			for i := 0; i < tt.count; i++ {
				consumerID := id.ID(fmt.Sprintf("consumer-%d", i))
				if err := manager.RemoveConsumer(consumerID); err != nil {
					t.Errorf("RemoveConsumer(%q) of %d consumers error = %v, want nil", consumerID, tt.count, err)
					return
				}
				if got, want := manager.Len(), uint64(tt.count-i-1); got != want {
					t.Errorf("Len() after removing %q from %d consumers = %d, want %d", consumerID, tt.count, got, want)
				}
			}
		})
	}
}

// TestManagerRejectsAdd checks that rejected additions preserve stored metadata.
func TestManagerRejectsAdd(t *testing.T) {
	original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "old/", ipcPath: "/consumer-1"}
	tests := []struct {
		name    string
		info    Info
		wantErr error
	}{
		{
			name:    "duplicate ID",
			info:    Info{id: original.id, topic: "other-topic", offset: 99, prefix: "new/", ipcPath: "/other"},
			wantErr: hashmap.ErrKeyExists,
		},
		{name: "empty ID", info: Info{}, wantErr: id.ErrInvalidID},
		{name: "invalid ID", info: Info{id: "invalid/id"}, wantErr: id.ErrInvalidID},
		{name: "ID too long", info: Info{id: id.ID(strings.Repeat("a", 33))}, wantErr: id.ErrInvalidID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := original
			original.ipcPath = newTestConsumerQueue(t)
			if original.ipcPath == "" {
				return
			}
			manager := newTestManager(t, original)
			if manager == nil {
				return
			}
			err := manager.AddConsumer(tt.info.id, tt.info.topic, tt.info.offset, tt.info.prefix, tt.info.ipcPath)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("AddConsumer(%+v) error = %v, want %v", tt.info, err, tt.wantErr)
			}
			if got := manager.Len(); got != 1 {
				t.Errorf("Len() after rejecting AddConsumer(%+v) = %d, want 1", tt.info, got)
			}
			stored := openTestConsumer(t, manager, original.id)
			if stored != nil && stored.Info != original {
				t.Errorf("OpenConsumer(%q).Info after rejecting AddConsumer(%+v) = %+v, want %+v", original.id, tt.info, stored.Info, original)
			}
		})
	}
}

// TestManagerNotFound checks missing lookups and removals without changing the index.
func TestManagerNotFound(t *testing.T) {
	tests := []struct {
		name  string
		infos []Info
	}{
		{name: "empty"},
		{name: "populated", infos: []Info{{id: "existing", topic: "events", ipcPath: "/existing"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newTestManager(t, tt.infos...)
			if manager == nil {
				return
			}
			consumerID := id.ID("missing")
			if c, err := manager.OpenConsumer(consumerID); c != nil || !errors.Is(err, ErrNotFound) {
				t.Errorf("OpenConsumer(%q) with %d stored consumers = %+v, error = %v, want nil and %v", consumerID, len(tt.infos), c, err, ErrNotFound)
			}
			if err := manager.UpdateOffset(consumerID, 8); !errors.Is(err, ErrNotFound) {
				t.Errorf("UpdateOffset(%q, 8) with %d stored consumers error = %v, want %v", consumerID, len(tt.infos), err, ErrNotFound)
			}
			if err := manager.UpdateTopic(consumerID, "other-topic"); !errors.Is(err, ErrNotFound) {
				t.Errorf("UpdateTopic(%q, %q) with %d stored consumers error = %v, want %v", consumerID, "other-topic", len(tt.infos), err, ErrNotFound)
			}
			if err := manager.RemoveConsumer(consumerID); !errors.Is(err, ErrNotFound) {
				t.Errorf("RemoveConsumer(%q) with %d stored consumers error = %v, want %v", consumerID, len(tt.infos), err, ErrNotFound)
			}
			if got := manager.Len(); got != uint64(len(tt.infos)) {
				t.Errorf("Len() after missing lookup and removal of %q = %d, want %d", consumerID, got, len(tt.infos))
			}
		})
	}
}

// TestManagerUpdateOffset checks exact offset storage and metadata isolation.
func TestManagerUpdateOffset(t *testing.T) {
	tests := []struct {
		name   string
		offset uint32
	}{
		{name: "acknowledgement", offset: 8},
		{name: "same offset", offset: 7},
		{name: "rewind", offset: 3},
		{name: "reset"},
		{name: "maximum offset", offset: ^uint32(0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := newTestConsumerQueue(t)
			if ipcPath == "" {
				return
			}
			original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: ipcPath}
			other := Info{id: "consumer-2", topic: "other-topic", offset: 11, ipcPath: ipcPath}
			manager := newTestManager(t, original, other)
			if manager == nil {
				return
			}
			previous := openTestConsumer(t, manager, original.id)
			if previous == nil {
				return
			}
			if err := manager.UpdateOffset(original.id, tt.offset); err != nil {
				t.Errorf("UpdateOffset(%q, %d) error = %v, want nil", original.id, tt.offset, err)
				return
			}
			want := original
			want.offset = tt.offset
			if got := openTestConsumer(t, manager, original.id); got != nil && got.Info != want {
				t.Errorf("OpenConsumer(%q).Info after UpdateOffset(%d) = %+v, want %+v", original.id, tt.offset, got.Info, want)
			}
			if previous.Info != original {
				t.Errorf("previous OpenConsumer(%q).Info after UpdateOffset(%d) = %+v, want %+v", original.id, tt.offset, previous.Info, original)
			}
			if got := openTestConsumer(t, manager, other.id); got != nil && got.Info != other {
				t.Errorf("OpenConsumer(%q).Info after UpdateOffset(%q, %d) = %+v, want %+v", other.id, original.id, tt.offset, got.Info, other)
			}
		})
	}
}

// TestManagerUpdateTopic checks topic replacement and metadata isolation.
func TestManagerUpdateTopic(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{name: "different topic", topic: "new-topic"},
		{name: "same topic", topic: "events"},
		{name: "minimum topic", topic: "t"},
		{name: "maximum topic", topic: strings.Repeat("t", 32)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := newTestConsumerQueue(t)
			if ipcPath == "" {
				return
			}
			original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: ipcPath}
			other := Info{id: "consumer-2", topic: "other-topic", offset: 11, ipcPath: ipcPath}
			manager := newTestManager(t, original, other)
			if manager == nil {
				return
			}
			previous := openTestConsumer(t, manager, original.id)
			if previous == nil {
				return
			}
			if err := manager.UpdateTopic(original.id, tt.topic); err != nil {
				t.Errorf("UpdateTopic(%q, %q) error = %v, want nil", original.id, tt.topic, err)
				return
			}
			want := original
			want.topic = tt.topic
			if got := openTestConsumer(t, manager, original.id); got != nil && got.Info != want {
				t.Errorf("OpenConsumer(%q).Info after UpdateTopic(%q) = %+v, want %+v", original.id, tt.topic, got.Info, want)
			}
			if previous.Info != original {
				t.Errorf("previous OpenConsumer(%q).Info after UpdateTopic(%q) = %+v, want %+v", original.id, tt.topic, previous.Info, original)
			}
			if got := openTestConsumer(t, manager, other.id); got != nil && got.Info != other {
				t.Errorf("OpenConsumer(%q).Info after UpdateTopic(%q, %q) = %+v, want %+v", other.id, original.id, tt.topic, got.Info, other)
			}
		})
	}
}

// TestManagerConcurrentUpdates checks simultaneous writes and metadata reads for one consumer.
func TestManagerConcurrentUpdates(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if manager == nil {
		return
	}
	const iterations = 64
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(3)
	go func() {
		defer group.Done()
		<-start
		for i := 0; i < iterations; i++ {
			if err := manager.UpdateOffset(original.id, uint32(i)); err != nil {
				t.Errorf("concurrent UpdateOffset(%q, %d) error = %v, want nil", original.id, i, err)
				return
			}
		}
	}()
	go func() {
		defer group.Done()
		<-start
		for i := 0; i < iterations; i++ {
			topic := fmt.Sprintf("topic-%d", i)
			if err := manager.UpdateTopic(original.id, topic); err != nil {
				t.Errorf("concurrent UpdateTopic(%q, %q) error = %v, want nil", original.id, topic, err)
				return
			}
		}
	}()
	go func() {
		defer group.Done()
		<-start
		for i := 0; i < iterations; i++ {
			if c := openTestConsumer(t, manager, original.id); c == nil {
				return
			} else if c.Info.id != original.id || c.Info.prefix != original.prefix || c.Info.ipcPath != original.ipcPath {
				t.Errorf("concurrent OpenConsumer(%q).Info = %+v, want ID %q, prefix %q and IPC path %q", original.id, c.Info, original.id, original.prefix, original.ipcPath)
			}
		}
	}()
	close(start)
	group.Wait()
	want := original
	want.offset = iterations - 1
	want.topic = fmt.Sprintf("topic-%d", iterations-1)
	if got := openTestConsumer(t, manager, original.id); got != nil && got.Info != want {
		t.Errorf("OpenConsumer(%q).Info after %d concurrent updates per field = %+v, want %+v", original.id, iterations, got.Info, want)
	}
}

// TestManagerOpenConsumerCopies checks metadata isolation and fresh connection resources.
func TestManagerOpenConsumerCopies(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if manager == nil {
		return
	}
	first := openTestConsumer(t, manager, original.id)
	second := openTestConsumer(t, manager, original.id)
	if first == nil || second == nil {
		return
	}
	if first == second {
		t.Errorf("OpenConsumer(%q) twice returned the same consumer %p, want distinct consumers", original.id, first)
	}
	if first.Connection == second.Connection {
		t.Errorf("OpenConsumer(%q) twice returned the same connection %p, want distinct connections", original.id, first.Connection)
	}
	if first.Connection.MessageChan == second.Connection.MessageChan {
		t.Errorf("OpenConsumer(%q) twice shared MessageChan %v, want distinct channels", original.id, first.Connection.MessageChan)
	}
	if first.Connection.AcknowledgedChan == second.Connection.AcknowledgedChan {
		t.Errorf("OpenConsumer(%q) twice shared AcknowledgedChan %v, want distinct channels", original.id, first.Connection.AcknowledgedChan)
	}
	if first.Connection.HeartbeatChan == second.Connection.HeartbeatChan {
		t.Errorf("OpenConsumer(%q) twice shared HeartbeatChan %v, want distinct channels", original.id, first.Connection.HeartbeatChan)
	}
	if first.Info != original {
		t.Errorf("OpenConsumer(%q).Info = %+v, want %+v", original.id, first.Info, original)
	}

	first.Info = Info{id: "changed", topic: "other-topic", offset: 99, prefix: "other/", ipcPath: "/other"}
	if second.Info != original {
		t.Errorf("second OpenConsumer(%q).Info after changing the first copy = %+v, want %+v", original.id, second.Info, original)
	}
	third := openTestConsumer(t, manager, original.id)
	if third != nil && third.Info != original {
		t.Errorf("OpenConsumer(%q).Info after changing a returned copy = %+v, want %+v", original.id, third.Info, original)
	}
}

// TestManagerRemoveKeepsConnection checks that metadata removal does not close an active connection.
func TestManagerRemoveKeepsConnection(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if manager == nil {
		return
	}
	active := openTestConsumer(t, manager, original.id)
	if active == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	active.Connection.Cancel = cancel
	if err := manager.RemoveConsumer(original.id); err != nil {
		t.Errorf("RemoveConsumer(%q) error = %v, want nil", original.id, err)
		return
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("RemoveConsumer(%q) changed the active connection context error to %v, want nil", original.id, err)
	}
	if c, err := manager.OpenConsumer(original.id); c != nil || !errors.Is(err, ErrNotFound) {
		t.Errorf("OpenConsumer(%q) after removal = %+v, error = %v, want nil and %v", original.id, c, err, ErrNotFound)
	}
	if err := active.Connection.Close(); err != nil {
		t.Errorf("Close() on the connection retained after RemoveConsumer(%q) error = %v, want nil", original.id, err)
	}
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("Close() on the connection retained after RemoveConsumer(%q): context error = %v, want %v", original.id, err, context.Canceled)
	}
	replacementIPCPath := newTestConsumerQueue(t, "replacement")
	if replacementIPCPath == "" {
		return
	}
	if err := manager.AddConsumer(original.id, "other-topic", 11, "new/", replacementIPCPath); err != nil {
		t.Errorf("AddConsumer(%q, %q, 11, %q, %q) after removal error = %v, want nil", original.id, "other-topic", "new/", replacementIPCPath, err)
		return
	}
	replacement := openTestConsumer(t, manager, original.id)
	if replacement == nil {
		return
	}
	want := Info{id: original.id, topic: "other-topic", offset: 11, prefix: "new/", ipcPath: replacementIPCPath}
	if replacement.Info != want {
		t.Errorf("OpenConsumer(%q).Info after adding the ID again = %+v, want %+v", original.id, replacement.Info, want)
	}
	if replacement.Connection == active.Connection {
		t.Errorf("OpenConsumer(%q) after adding the ID again reused connection %p, want a new connection", original.id, active.Connection)
	}
}

// TestManagerConcurrent checks concurrent additions, retrievals, lengths, and removals.
func TestManagerConcurrent(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	manager := newTestManager(t)
	if manager == nil {
		return
	}
	const goroutines, iterations = 8, 16
	start := make(chan struct{})
	var group sync.WaitGroup
	for worker := 0; worker < goroutines; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			<-start
			for i := 0; i < iterations; i++ {
				consumerID := id.ID(fmt.Sprintf("consumer-%d-%d", worker, i))
				want := Info{id: consumerID, topic: fmt.Sprintf("topic-%d", worker), offset: uint32(i), prefix: "events/", ipcPath: ipcPath}
				if err := manager.AddConsumer(want.id, want.topic, want.offset, want.prefix, want.ipcPath); err != nil {
					t.Errorf("concurrent AddConsumer(%+v) error = %v, want nil", want, err)
					return
				}
				c := openTestConsumer(t, manager, consumerID)
				if c == nil {
					return
				}
				if c.Info != want {
					t.Errorf("concurrent OpenConsumer(%q).Info = %+v, want %+v", consumerID, c.Info, want)
				}
				if got := manager.Len(); got == 0 {
					t.Errorf("concurrent Len() while consumer %q is present = %d, want at least 1", consumerID, got)
				}
				if err := manager.RemoveConsumer(consumerID); err != nil {
					t.Errorf("concurrent RemoveConsumer(%q) error = %v, want nil", consumerID, err)
					return
				}
			}
		}(worker)
	}
	close(start)
	group.Wait()
	if got := manager.Len(); got != 0 {
		t.Errorf("Len() after %d goroutines each completed %d add/get/remove sequences = %d, want 0", goroutines, iterations, got)
	}
}
