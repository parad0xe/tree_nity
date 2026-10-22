package consumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"tree_nity/src/ipc"
	"tree_nity/src/server/utils"
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
		if err := manager.AddConsumer(info.id, info.offset, info.prefix, info.ipcPath); err != nil {
			t.Errorf("AddConsumer(%q, %d, %q, %q) during setup error = %v, want nil", info.id, info.offset, info.prefix, info.ipcPath, err)
			return nil
		}
	}
	return manager
}

func getTestConsumer(t *testing.T, manager *Manager, consumerID id.ID) *Consumer {
	t.Helper()
	c, err := manager.GetConsumer(consumerID)
	if err != nil || c == nil || c.Connection == nil {
		t.Errorf("GetConsumer(%q) = %+v, error = %v, want a consumer with a connection and nil error", consumerID, c, err)
		return nil
	}
	cleanupTestConnection(t, c.Connection)
	return c
}

// TestManagerGetConsumerPropagatesConnectionError checks initialization errors and metadata ownership.
func TestManagerGetConsumerPropagatesConnectionError(t *testing.T) {
	original := Info{id: "consumer-1", offset: 7, prefix: "events/", ipcPath: "/consumer-1"}
	manager := newTestManager(t, original)
	if manager == nil {
		return
	}
	c, err := manager.GetConsumer(original.id)
	if c != nil {
		cleanupTestConnection(t, c.Connection)
		t.Errorf("GetConsumer(%q) = %+v, want nil on connection initialization failure", original.id, c)
	}
	if !errors.Is(err, ipc.ErrNilEncoder) {
		t.Errorf("GetConsumer(%q) error = %v, want %v", original.id, err, ipc.ErrNilEncoder)
	}
	stored, exists := manager.consumerTable.Get(original.id)
	if !exists || stored == nil || *stored != original {
		t.Errorf("stored metadata for %q after failed GetConsumer = %+v, exists = %t, want %+v and true", original.id, stored, exists, original)
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
			if err := tt.manager.AddConsumer(consumerID, 7, "events/", "/consumer-1"); !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("AddConsumer(%q, 7, %q, %q) on %s error = %v, want %v", consumerID, "events/", "/consumer-1", tt.name, err, ErrManagerUninitialized)
			}
			if c, err := tt.manager.GetConsumer(consumerID); c != nil || !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("GetConsumer(%q) on %s = %+v, error = %v, want nil and %v", consumerID, tt.name, c, err, ErrManagerUninitialized)
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
				if err := manager.AddConsumer(consumerID, uint32(i), "events/", "/consumer-tests"); err != nil {
					t.Errorf("AddConsumer(%q, %d, %q, %q) of %d consumers error = %v, want nil", consumerID, i, "events/", "/consumer-tests", tt.count, err)
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
	original := Info{id: "consumer-1", offset: 7, prefix: "old/", ipcPath: "/consumer-1"}
	tests := []struct {
		name        string
		info        Info
		wantErr     error
		wantMessage string
	}{
		{
			name:    "duplicate ID",
			info:    Info{id: original.id, offset: 99, prefix: "new/", ipcPath: "/other"},
			wantErr: hashmap.ErrKeyExists,
		},
		{name: "empty ID", info: Info{}, wantMessage: "invalid consumer ID"},
		{name: "invalid ID", info: Info{id: "invalid/id"}, wantMessage: "invalid consumer ID"},
		{name: "ID too long", info: Info{id: id.ID(strings.Repeat("a", 33))}, wantMessage: "invalid consumer ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newTestManager(t, original)
			if manager == nil {
				return
			}
			err := manager.AddConsumer(tt.info.id, tt.info.offset, tt.info.prefix, tt.info.ipcPath)
			if tt.wantMessage != "" {
				if err == nil || err.Error() != tt.wantMessage {
					t.Errorf("AddConsumer(%+v) error = %v, want %q", tt.info, err, tt.wantMessage)
				}
			} else if !errors.Is(err, tt.wantErr) {
				t.Errorf("AddConsumer(%+v) error = %v, want %v", tt.info, err, tt.wantErr)
			}
			if got := manager.Len(); got != 1 {
				t.Errorf("Len() after rejecting AddConsumer(%+v) = %d, want 1", tt.info, got)
			}
			stored := getTestConsumer(t, manager, original.id)
			if stored != nil && stored.Info != original {
				t.Errorf("GetConsumer(%q).Info after rejecting AddConsumer(%+v) = %+v, want %+v", original.id, tt.info, stored.Info, original)
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
		{name: "populated", infos: []Info{{id: "existing", ipcPath: "/existing"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := newTestManager(t, tt.infos...)
			if manager == nil {
				return
			}
			consumerID := id.ID("missing")
			if c, err := manager.GetConsumer(consumerID); c != nil || !errors.Is(err, ErrNotFound) {
				t.Errorf("GetConsumer(%q) with %d stored consumers = %+v, error = %v, want nil and %v", consumerID, len(tt.infos), c, err, ErrNotFound)
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

// TestManagerGetConsumerCopies checks metadata isolation and fresh connection resources.
func TestManagerGetConsumerCopies(t *testing.T) {
	original := Info{id: "consumer-1", offset: 7, prefix: "events/", ipcPath: "/consumer-1"}
	manager := newTestManager(t, original)
	if manager == nil {
		return
	}
	first := getTestConsumer(t, manager, original.id)
	second := getTestConsumer(t, manager, original.id)
	if first == nil || second == nil {
		return
	}
	if first == second {
		t.Errorf("GetConsumer(%q) twice returned the same consumer %p, want distinct consumers", original.id, first)
	}
	if first.Connection == second.Connection {
		t.Errorf("GetConsumer(%q) twice returned the same connection %p, want distinct connections", original.id, first.Connection)
	}
	if first.Connection.MessageChan == second.Connection.MessageChan {
		t.Errorf("GetConsumer(%q) twice shared MessageChan %v, want distinct channels", original.id, first.Connection.MessageChan)
	}
	if first.Connection.AcknowledgedChan == second.Connection.AcknowledgedChan {
		t.Errorf("GetConsumer(%q) twice shared AcknowledgedChan %v, want distinct channels", original.id, first.Connection.AcknowledgedChan)
	}
	if first.Connection.HeartbeatChan == second.Connection.HeartbeatChan {
		t.Errorf("GetConsumer(%q) twice shared HeartbeatChan %v, want distinct channels", original.id, first.Connection.HeartbeatChan)
	}
	if first.Info != original {
		t.Errorf("GetConsumer(%q).Info = %+v, want %+v", original.id, first.Info, original)
	}

	first.Info = Info{id: "changed", offset: 99, prefix: "other/", ipcPath: "/other"}
	if second.Info != original {
		t.Errorf("second GetConsumer(%q).Info after changing the first copy = %+v, want %+v", original.id, second.Info, original)
	}
	third := getTestConsumer(t, manager, original.id)
	if third != nil && third.Info != original {
		t.Errorf("GetConsumer(%q).Info after changing a returned copy = %+v, want %+v", original.id, third.Info, original)
	}
}

// TestManagerRemoveKeepsConnection checks that metadata removal does not close an active connection.
func TestManagerRemoveKeepsConnection(t *testing.T) {
	original := Info{id: "consumer-1", offset: 7, prefix: "events/", ipcPath: "/consumer-1"}
	manager := newTestManager(t, original)
	if manager == nil {
		return
	}
	active := getTestConsumer(t, manager, original.id)
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
	if c, err := manager.GetConsumer(original.id); c != nil || !errors.Is(err, ErrNotFound) {
		t.Errorf("GetConsumer(%q) after removal = %+v, error = %v, want nil and %v", original.id, c, err, ErrNotFound)
	}
	if err := active.Connection.Close(); err != nil {
		t.Errorf("Close() on the connection retained after RemoveConsumer(%q) error = %v, want nil", original.id, err)
	}
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("Close() on the connection retained after RemoveConsumer(%q): context error = %v, want %v", original.id, err, context.Canceled)
	}
	if err := manager.AddConsumer(original.id, 11, "new/", "/consumer-new"); err != nil {
		t.Errorf("AddConsumer(%q, 11, %q, %q) after removal error = %v, want nil", original.id, "new/", "/consumer-new", err)
		return
	}
	replacement := getTestConsumer(t, manager, original.id)
	if replacement == nil {
		return
	}
	want := Info{id: original.id, offset: 11, prefix: "new/", ipcPath: "/consumer-new"}
	if replacement.Info != want {
		t.Errorf("GetConsumer(%q).Info after adding the ID again = %+v, want %+v", original.id, replacement.Info, want)
	}
	if replacement.Connection == active.Connection {
		t.Errorf("GetConsumer(%q) after adding the ID again reused connection %p, want a new connection", original.id, active.Connection)
	}
}

// TestManagerConcurrent checks concurrent additions, retrievals, lengths, and removals.
func TestManagerConcurrent(t *testing.T) {
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
				want := Info{id: consumerID, offset: uint32(i), prefix: "events/", ipcPath: "/consumer-tests"}
				if err := manager.AddConsumer(want.id, want.offset, want.prefix, want.ipcPath); err != nil {
					t.Errorf("concurrent AddConsumer(%+v) error = %v, want nil", want, err)
					return
				}
				c := getTestConsumer(t, manager, consumerID)
				if c == nil {
					return
				}
				if c.Info != want {
					t.Errorf("concurrent GetConsumer(%q).Info = %+v, want %+v", consumerID, c.Info, want)
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
