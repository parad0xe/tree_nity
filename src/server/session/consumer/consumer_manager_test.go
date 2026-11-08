package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/server/utils/hashmap"
)

func newTestManager(t *testing.T, infos ...Info) *Manager {
	t.Helper()
	manager := NewManager()
	if err := manager.validate(); err != nil {
		t.Errorf("NewManager() validation error = %v, want nil", err)
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
	c, err := manager.GetOrCreateConsumer(consumerID)
	if err != nil || c == nil {
		t.Errorf("GetOrCreateConsumer(%q) = %+v, error = %v, want a consumer and nil error", consumerID, c, err)
		return nil
	}
	t.Cleanup(func() {
		if err := manager.DisconnectConsumer(c); err != nil {
			t.Errorf("DisconnectConsumer() for consumer %q error = %v, want nil", consumerID, err)
		}
	})
	return c
}

func connectTestConsumer(t *testing.T, manager *Manager, consumerID id.ID) *Consumer {
	t.Helper()
	c := openTestConsumer(t, manager, consumerID)
	if c == nil {
		return nil
	}
	if err := manager.ConnectConsumer(c); err != nil {
		t.Errorf("ConnectConsumer() for consumer %q error = %v, want nil", consumerID, err)
		return nil
	}
	if c.Connection() == nil {
		t.Errorf("Connection() for consumer %q after ConnectConsumer() = nil, want an initialized connection", consumerID)
		return nil
	}
	return c
}

// TestManagerGetOrCreateConsumerWithoutIPC checks lookup without a queue.
func TestManagerGetOrCreateConsumerWithoutIPC(t *testing.T) {
	tests := []struct {
		name    string
		ipcPath string
	}{
		{name: "invalid name", ipcPath: "/invalid/queue"},
		{name: "missing queue", ipcPath: fmt.Sprintf("/consumer.%s.%d", t.Name(), os.Getpid())},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: tt.ipcPath}
			manager := newTestManager(t, original)
			if err := manager.validate(); err != nil {
				return
			}
			c := openTestConsumer(t, manager, original.id)
			if c == nil {
				return
			}
			if c.Info != original {
				t.Errorf("GetOrCreateConsumer(%q) with queue %q metadata = %+v, want %+v", original.id, tt.ipcPath, c.Info, original)
			}
			if connection := c.Connection(); connection != nil {
				t.Errorf("Connection() after GetOrCreateConsumer(%q) with queue %q = %p, want nil", original.id, tt.ipcPath, connection)
			}
			if err := c.Validate(); err != nil {
				t.Errorf("Validate() after GetOrCreateConsumer(%q) with queue %q error = %v, want nil", original.id, tt.ipcPath, err)
			}
		})
	}
}

// TestManagerConnectPropagatesConnectionError checks queue error propagation.
func TestManagerConnectPropagatesConnectionError(t *testing.T) {
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
			if err := manager.validate(); err != nil {
				return
			}
			c := openTestConsumer(t, manager, original.id)
			if c == nil {
				return
			}
			if err := manager.ConnectConsumer(c); !errors.Is(err, tt.wantErr) {
				t.Errorf("ConnectConsumer() for consumer %q with queue %q error = %v, want %v", original.id, tt.ipcPath, err, tt.wantErr)
			}
			if connection := c.Connection(); connection != nil {
				t.Errorf("Connection() for consumer %q after failed ConnectConsumer() with queue %q = %p, want nil", original.id, tt.ipcPath, connection)
			}
			stored, exists := manager.consumerTable.Get(original.id)
			if !exists || stored == nil || *stored != original {
				t.Errorf("stored metadata for %q after failed ConnectConsumer() with queue %q = %+v, exists = %t, want %+v and true", original.id, tt.ipcPath, stored, exists, original)
			}
			if got := manager.activeConsumers.Len(); got != 0 {
				t.Errorf("active connections after failed ConnectConsumer() for %q with queue %q = %d, want 0", original.id, tt.ipcPath, got)
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
			if c, err := tt.manager.GetOrCreateConsumer(consumerID); c != nil || !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("GetOrCreateConsumer(%q) on %s = %+v, error = %v, want nil and %v", consumerID, tt.name, c, err, ErrManagerUninitialized)
			}
			c := newConsumer(newInfo(consumerID, "events", 7, "events/", "/consumer-1"))
			if err := tt.manager.ConnectConsumer(c); !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("ConnectConsumer(%q) on %s error = %v, want %v", consumerID, tt.name, err, ErrManagerUninitialized)
			}
			if err := tt.manager.DisconnectConsumer(c); !errors.Is(err, ErrManagerUninitialized) {
				t.Errorf("DisconnectConsumer(%q) on %s error = %v, want %v", consumerID, tt.name, err, ErrManagerUninitialized)
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
			if err := manager.validate(); err != nil {
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
			if err := manager.validate(); err != nil {
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
				t.Errorf("GetOrCreateConsumer(%q).Info after rejecting AddConsumer(%+v) = %+v, want %+v", original.id, tt.info, stored.Info, original)
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
			if err := manager.validate(); err != nil {
				return
			}
			consumerID := id.ID("missing")
			if c, err := manager.GetOrCreateConsumer(consumerID); c != nil || !errors.Is(err, ErrNotFound) {
				t.Errorf("GetOrCreateConsumer(%q) with %d stored consumers = %+v, error = %v, want nil and %v", consumerID, len(tt.infos), c, err, ErrNotFound)
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
			if err := manager.validate(); err != nil {
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
				t.Errorf("GetOrCreateConsumer(%q).Info after UpdateOffset(%d) = %+v, want %+v", original.id, tt.offset, got.Info, want)
			}
			if previous.Info != original {
				t.Errorf("previous GetOrCreateConsumer(%q).Info after UpdateOffset(%d) = %+v, want %+v", original.id, tt.offset, previous.Info, original)
			}
			if got := openTestConsumer(t, manager, other.id); got != nil && got.Info != other {
				t.Errorf("GetOrCreateConsumer(%q).Info after UpdateOffset(%q, %d) = %+v, want %+v", other.id, original.id, tt.offset, got.Info, other)
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
			if err := manager.validate(); err != nil {
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
				t.Errorf("GetOrCreateConsumer(%q).Info after UpdateTopic(%q) = %+v, want %+v", original.id, tt.topic, got.Info, want)
			}
			if previous.Info != original {
				t.Errorf("previous GetOrCreateConsumer(%q).Info after UpdateTopic(%q) = %+v, want %+v", original.id, tt.topic, previous.Info, original)
			}
			if got := openTestConsumer(t, manager, other.id); got != nil && got.Info != other {
				t.Errorf("GetOrCreateConsumer(%q).Info after UpdateTopic(%q, %q) = %+v, want %+v", other.id, original.id, tt.topic, got.Info, other)
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
	if err := manager.validate(); err != nil {
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
				t.Errorf("concurrent GetOrCreateConsumer(%q).Info = %+v, want ID %q, prefix %q and IPC path %q", original.id, c.Info, original.id, original.prefix, original.ipcPath)
			}
		}
	}()
	close(start)
	group.Wait()
	want := original
	want.offset = iterations - 1
	want.topic = fmt.Sprintf("topic-%d", iterations-1)
	if got := openTestConsumer(t, manager, original.id); got != nil && got.Info != want {
		t.Errorf("GetOrCreateConsumer(%q).Info after %d concurrent updates per field = %+v, want %+v", original.id, iterations, got.Info, want)
	}
}

// TestManagerGetOrCreateConsumerCopies checks metadata isolation.
func TestManagerGetOrCreateConsumerCopies(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if err := manager.validate(); err != nil {
		return
	}
	first := openTestConsumer(t, manager, original.id)
	second := openTestConsumer(t, manager, original.id)
	if first == nil || second == nil {
		return
	}
	if first == second {
		t.Errorf("GetOrCreateConsumer(%q) twice returned the same consumer %p, want distinct consumers", original.id, first)
	}
	if first.Connection() != nil || second.Connection() != nil {
		t.Errorf("Connection() after GetOrCreateConsumer(%q) twice = (%p, %p), want (nil, nil)", original.id, first.Connection(), second.Connection())
	}
	if first.Info != original {
		t.Errorf("GetOrCreateConsumer(%q).Info = %+v, want %+v", original.id, first.Info, original)
	}

	first.Info = Info{id: "changed", topic: "other-topic", offset: 99, prefix: "other/", ipcPath: "/other"}
	if second.Info != original {
		t.Errorf("second GetOrCreateConsumer(%q).Info after changing the first copy = %+v, want %+v", original.id, second.Info, original)
	}
	third := openTestConsumer(t, manager, original.id)
	if third != nil && third.Info != original {
		t.Errorf("GetOrCreateConsumer(%q).Info after changing a returned copy = %+v, want %+v", original.id, third.Info, original)
	}
}

// TestManagerConnectRejectsDuplicateID checks duplicate connection rejection.
func TestManagerConnectRejectsDuplicateID(t *testing.T) {
	tests := []struct {
		name     string
		sameCopy bool
	}{
		{name: "same consumer", sameCopy: true},
		{name: "another metadata copy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := newTestConsumerQueue(t)
			if ipcPath == "" {
				return
			}
			original := Info{id: "consumer-1", topic: "events", ipcPath: ipcPath}
			manager := newTestManager(t, original)
			if err := manager.validate(); err != nil {
				return
			}
			first := connectTestConsumer(t, manager, original.id)
			if first == nil {
				return
			}
			connection := first.Connection()
			second := first
			if !tt.sameCopy {
				second = openTestConsumer(t, manager, original.id)
				if second == nil {
					return
				}
			}
			if err := manager.ConnectConsumer(second); !errors.Is(err, ErrAlreadyConnected) {
				t.Errorf("ConnectConsumer() for %s with active ID %q error = %v, want %v", tt.name, original.id, err, ErrAlreadyConnected)
			}
			if got := first.Connection(); got != connection {
				t.Errorf("Connection() for active ID %q after rejected ConnectConsumer() = %p, want %p", original.id, got, connection)
			}
			if !tt.sameCopy && second.Connection() != nil {
				t.Errorf("Connection() for another copy of active ID %q after rejected ConnectConsumer() = %p, want nil", original.id, second.Connection())
			}
		})
	}
}

// TestManagerConnectRetriesAfterQueueAppears checks retry after failure.
func TestManagerConnectRetriesAfterQueueAppears(t *testing.T) {
	tests := []struct {
		name     string
		sameCopy bool
	}{
		{name: "same consumer", sameCopy: true},
		{name: "another metadata copy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := fmt.Sprintf("/consumer.%s.%d.", strings.ReplaceAll(t.Name(), "/", "."), os.Getpid())
			original := Info{id: "consumer-1", topic: "events", ipcPath: ipcPath}
			manager := newTestManager(t, original)
			if err := manager.validate(); err != nil {
				return
			}
			first := openTestConsumer(t, manager, original.id)
			if first == nil {
				return
			}
			if err := manager.ConnectConsumer(first); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("ConnectConsumer() for consumer %q with missing queue %q error = %v, want %v", original.id, ipcPath, err, os.ErrNotExist)
				return
			}
			if got := newTestConsumerQueue(t); got != ipcPath {
				t.Errorf("newTestConsumerQueue() = %q, want %q for consumer %q", got, ipcPath, original.id)
				return
			}
			second := first
			if !tt.sameCopy {
				second = openTestConsumer(t, manager, original.id)
				if second == nil {
					return
				}
			}
			if err := manager.ConnectConsumer(second); err != nil {
				t.Errorf("ConnectConsumer() for %s of ID %q after creating queue %q error = %v, want nil", tt.name, original.id, ipcPath, err)
			}
		})
	}
}

// TestManagerConnectRejectsRemovedConsumer checks connection after removal.
func TestManagerConnectRejectsRemovedConsumer(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if err := manager.validate(); err != nil {
		return
	}
	c := openTestConsumer(t, manager, original.id)
	if c == nil {
		return
	}
	if err := manager.RemoveConsumer(original.id); err != nil {
		t.Errorf("RemoveConsumer(%q) error = %v, want nil", original.id, err)
		return
	}
	if err := manager.ConnectConsumer(c); !errors.Is(err, ErrNotFound) {
		t.Errorf("ConnectConsumer() for removed consumer %q error = %v, want %v", original.id, err, ErrNotFound)
	}
}

// TestManagerConnectRejectsReaddedActiveID checks ID reservation after replacement.
func TestManagerConnectRejectsReaddedActiveID(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if err := manager.validate(); err != nil {
		return
	}
	first := connectTestConsumer(t, manager, original.id)
	if first == nil {
		return
	}
	if err := manager.RemoveConsumer(original.id); err != nil {
		t.Errorf("RemoveConsumer(%q) with an active connection error = %v, want nil", original.id, err)
		return
	}
	if err := manager.AddConsumer(original.id, "other-topic", 11, "new/", ipcPath); err != nil {
		t.Errorf("AddConsumer(%q, %q, 11, %q, %q) with an old active connection error = %v, want nil", original.id, "other-topic", "new/", ipcPath, err)
		return
	}
	second := openTestConsumer(t, manager, original.id)
	if second == nil {
		return
	}
	if err := manager.ConnectConsumer(second); !errors.Is(err, ErrAlreadyConnected) {
		t.Errorf("ConnectConsumer() for readded ID %q with an old active connection error = %v, want %v", original.id, err, ErrAlreadyConnected)
	}
	if err := manager.DisconnectConsumer(first); err != nil {
		t.Errorf("DisconnectConsumer() for old connection of readded ID %q error = %v, want nil", original.id, err)
		return
	}
	if err := manager.ConnectConsumer(second); err != nil {
		t.Errorf("ConnectConsumer() for readded ID %q after old connection disconnected error = %v, want nil", original.id, err)
	}
}

// TestManagerDisconnectKeepsOtherConnection checks connection ownership.
func TestManagerDisconnectKeepsOtherConnection(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if err := manager.validate(); err != nil {
		return
	}
	first := connectTestConsumer(t, manager, original.id)
	if first == nil {
		return
	}
	if err := manager.DisconnectConsumer(first); err != nil {
		t.Errorf("DisconnectConsumer() for first copy of %q error = %v, want nil", original.id, err)
		return
	}
	second := connectTestConsumer(t, manager, original.id)
	if second == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	second.Connection().Cancel = cancel
	if err := manager.DisconnectConsumer(first); err != nil {
		t.Errorf("DisconnectConsumer() again for first copy of %q error = %v, want nil", original.id, err)
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("context error for second copy of %q after first copy disconnected again = %v, want nil", original.id, err)
	}
	if err := manager.ConnectConsumer(first); !errors.Is(err, ErrAlreadyConnected) {
		t.Errorf("ConnectConsumer() for first copy of %q while second copy is active error = %v, want %v", original.id, err, ErrAlreadyConnected)
	}
}

// TestManagerConnectionLifecycle checks independent connections and reservation cleanup.
func TestManagerConnectionLifecycle(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "empty"},
		{name: "single consumer", count: 1},
		{name: "many consumers", count: 64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := newTestConsumerQueue(t)
			if ipcPath == "" {
				return
			}
			manager := newTestManager(t)
			if err := manager.validate(); err != nil {
				return
			}
			consumers := make([]*Consumer, tt.count)
			for i := range consumers {
				consumerID := id.ID(fmt.Sprintf("consumer-%d", i))
				if err := manager.AddConsumer(consumerID, "events", 0, "events/", ipcPath); err != nil {
					t.Errorf("AddConsumer(%q, %q, 0, %q, %q) of %d consumers error = %v, want nil", consumerID, "events", "events/", ipcPath, tt.count, err)
					return
				}
				consumers[i] = connectTestConsumer(t, manager, consumerID)
				if err := consumers[i].Validate(); err != nil {
					t.Errorf("Validate() after connecting consumer %q of %d consumers error = %v, want nil", consumerID, tt.count, err)
					return
				}
			}
			if got := manager.activeConsumers.Len(); got != uint64(tt.count) {
				t.Errorf("active connection count after connecting %d consumers = %d, want %d", tt.count, got, tt.count)
			}
			for i, c := range consumers {
				if err := manager.DisconnectConsumer(c); err != nil {
					t.Errorf("DisconnectConsumer() for consumer %q of %d consumers error = %v, want nil", c.Info.id, tt.count, err)
				}
				if got, want := manager.activeConsumers.Len(), uint64(tt.count-i-1); got != want {
					t.Errorf("active connection count after disconnecting %q from %d consumers = %d, want %d", c.Info.id, tt.count, got, want)
				}
			}
		})
	}
}

// TestManagerConcurrentConnect checks exclusive connection ownership.
func TestManagerConcurrentConnect(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if err := manager.validate(); err != nil {
		return
	}
	const count = 32
	consumers := make([]*Consumer, count)
	for i := range consumers {
		consumers[i] = openTestConsumer(t, manager, original.id)
		if consumers[i] == nil {
			return
		}
	}
	start := make(chan struct{})
	errorsByCopy := make([]error, count)
	var group sync.WaitGroup
	for i, c := range consumers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			errorsByCopy[i] = manager.ConnectConsumer(c)
		}()
	}
	close(start)
	group.Wait()
	connected := 0
	for i, err := range errorsByCopy {
		if err == nil {
			connected++
		} else if !errors.Is(err, ErrAlreadyConnected) {
			t.Errorf("concurrent ConnectConsumer() for copy %d of %q error = %v, want nil or %v", i, original.id, err, ErrAlreadyConnected)
		}
	}
	if connected != 1 {
		t.Errorf("concurrent ConnectConsumer() for %d copies of %q connected %d consumers, want 1", count, original.id, connected)
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
	if err := manager.validate(); err != nil {
		return
	}
	active := connectTestConsumer(t, manager, original.id)
	if active == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	active.Connection().Cancel = cancel
	if err := manager.RemoveConsumer(original.id); err != nil {
		t.Errorf("RemoveConsumer(%q) error = %v, want nil", original.id, err)
		return
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("RemoveConsumer(%q) changed the active connection context error to %v, want nil", original.id, err)
	}
	if c, err := manager.GetOrCreateConsumer(original.id); c != nil || !errors.Is(err, ErrNotFound) {
		t.Errorf("GetOrCreateConsumer(%q) after removal = %+v, error = %v, want nil and %v", original.id, c, err, ErrNotFound)
	}
	previousConnection := active.Connection()
	if err := manager.DisconnectConsumer(active); err != nil {
		t.Errorf("DisconnectConsumer() on the consumer retained after RemoveConsumer(%q) error = %v, want nil", original.id, err)
	}
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("DisconnectConsumer() on the consumer retained after RemoveConsumer(%q): context error = %v, want %v", original.id, err, context.Canceled)
	}
	replacementIPCPath := newTestConsumerQueue(t, "replacement")
	if replacementIPCPath == "" {
		return
	}
	if err := manager.AddConsumer(original.id, "other-topic", 11, "new/", replacementIPCPath); err != nil {
		t.Errorf("AddConsumer(%q, %q, 11, %q, %q) after removal error = %v, want nil", original.id, "other-topic", "new/", replacementIPCPath, err)
		return
	}
	replacement := connectTestConsumer(t, manager, original.id)
	if replacement == nil {
		return
	}
	want := Info{id: original.id, topic: "other-topic", offset: 11, prefix: "new/", ipcPath: replacementIPCPath}
	if replacement.Info != want {
		t.Errorf("GetOrCreateConsumer(%q).Info after adding the ID again = %+v, want %+v", original.id, replacement.Info, want)
	}
	if replacement.Connection() == previousConnection {
		t.Errorf("GetOrCreateConsumer(%q) after adding the ID again reused connection %p, want a new connection", original.id, previousConnection)
	}
}

// TestManagerConcurrent checks concurrent additions, retrievals, lengths, and removals.
func TestManagerConcurrent(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	manager := newTestManager(t)
	if err := manager.validate(); err != nil {
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
					t.Errorf("concurrent GetOrCreateConsumer(%q).Info = %+v, want %+v", consumerID, c.Info, want)
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

// TestManagerDisconnectMissingResources checks cleanup without resources.
func TestManagerDisconnectMissingResources(t *testing.T) {
	manager := NewManager()
	tests := []struct {
		name     string
		consumer *Consumer
	}{
		{name: "nil receiver"},
		{name: "zero value", consumer: &Consumer{}},
		{name: "empty connection", consumer: &Consumer{manager: manager, connection: &Connection{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 2; i++ {
				if err := manager.DisconnectConsumer(tt.consumer); err != nil {
					t.Errorf("DisconnectConsumer() call %d for %s error = %v, want nil", i+1, tt.name, err)
				}
			}
		})
	}
}

// TestManagerDisconnectCancelsContext checks cleanup with invalid metadata.
func TestManagerDisconnectCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	manager := NewManager()
	c := &Consumer{manager: manager, connection: &Connection{Cancel: cancel}}
	if err := manager.DisconnectConsumer(c); err != nil {
		t.Errorf("DisconnectConsumer() with zero metadata error = %v, want nil", err)
	}
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("DisconnectConsumer() with zero metadata: context error = %v, want %v", err, context.Canceled)
	}
}

// TestManagerDisconnectAtFunctionReturn checks deferred disconnection.
func TestManagerDisconnectAtFunctionReturn(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if err := manager.validate(); err != nil {
		return
	}
	c := connectTestConsumer(t, manager, original.id)
	if err := c.Validate(); err != nil {
		return
	}
	connection := func() *Connection {
		defer func() {
			if err := manager.DisconnectConsumer(c); err != nil {
				t.Errorf("deferred DisconnectConsumer() for consumer %q error = %v, want nil", original.id, err)
			}
		}()
		return c.Connection()
	}()
	if connection == nil {
		return
	}
	metadata := protocol.NewClientMetadata(string(original.id), ipcPath)
	payload := protocol.NewConsumerHeartbeatPayload()
	data, err := protocol.NewData(protocol.ConsumerHeartbeat, metadata, payload)
	if err != nil {
		t.Errorf("NewData(%v, %+v, %+v) error = %v, want nil", protocol.ConsumerHeartbeat, metadata, payload, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	if err := connection.Outbound.Send(ctx, data); !errors.Is(err, syscall.EBADF) {
		t.Errorf("Send(%+v) to %q after deferred DisconnectConsumer() error = %v, want %v", data, ipcPath, err, syscall.EBADF)
	}
	if err := manager.DisconnectConsumer(c); err != nil {
		t.Errorf("repeated DisconnectConsumer() for consumer %q error = %v, want nil", original.id, err)
	}
	sender, err := mqueue.NewSender(ipcPath)
	if err != nil {
		t.Errorf("NewSender(%q) after deferred DisconnectConsumer() error = %v, want nil because the receiver still owns the queue", ipcPath, err)
		return
	}
	if err := sender.Close(); err != nil {
		t.Errorf("Close() for replacement sender %q error = %v, want nil", ipcPath, err)
	}
}

// TestManagerConnectRejectsInvalidConsumer checks invalid consumers.
func TestManagerConnectRejectsInvalidConsumer(t *testing.T) {
	manager := NewManager()
	tests := []struct {
		name     string
		consumer *Consumer
		wantErr  error
	}{
		{name: "nil receiver", wantErr: ErrUninitialized},
		{name: "zero value", consumer: &Consumer{}, wantErr: id.ErrInvalidID},
		{name: "invalid ID", consumer: &Consumer{Info: Info{id: "invalid/id"}}, wantErr: id.ErrInvalidID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := manager.ConnectConsumer(tt.consumer); !errors.Is(err, tt.wantErr) {
				t.Errorf("ConnectConsumer() for %s error = %v, want %v", tt.name, err, tt.wantErr)
			}
			if connection := tt.consumer.Connection(); connection != nil {
				t.Errorf("Connection() for %s after failed ConnectConsumer() = %p, want nil", tt.name, connection)
			}
		})
	}
}

// TestManagerDisconnectWithoutConnection checks repeated disconnection.
func TestManagerDisconnectWithoutConnection(t *testing.T) {
	manager := NewManager()
	tests := []struct {
		name     string
		consumer *Consumer
	}{
		{name: "nil receiver"},
		{name: "zero value", consumer: &Consumer{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 2; i++ {
				if err := manager.DisconnectConsumer(tt.consumer); err != nil {
					t.Errorf("DisconnectConsumer() call %d for %s error = %v, want nil", i+1, tt.name, err)
				}
			}
		})
	}
}

// TestManagerReconnect checks resource renewal and metadata preservation.
func TestManagerReconnect(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if err := manager.validate(); err != nil {
		return
	}
	c := connectTestConsumer(t, manager, original.id)
	if c == nil {
		return
	}
	first := c.Connection()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	first.Cancel = cancel
	for i := 0; i < 2; i++ {
		if err := manager.DisconnectConsumer(c); err != nil {
			t.Errorf("DisconnectConsumer() call %d for consumer %q error = %v, want nil", i+1, original.id, err)
		}
	}
	if connection := c.Connection(); connection != nil {
		t.Errorf("Connection() after DisconnectConsumer() for consumer %q = %p, want nil", original.id, connection)
	}
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("context error after DisconnectConsumer() for consumer %q = %v, want %v", original.id, err, context.Canceled)
	}
	if c.Info != original {
		t.Errorf("Info after DisconnectConsumer() for consumer %q = %+v, want %+v", original.id, c.Info, original)
	}
	if stored := openTestConsumer(t, manager, original.id); stored != nil && stored.Info != original {
		t.Errorf("GetOrCreateConsumer(%q).Info after DisconnectConsumer() = %+v, want %+v", original.id, stored.Info, original)
	}
	if err := manager.ConnectConsumer(c); err != nil {
		t.Errorf("ConnectConsumer() for consumer %q after DisconnectConsumer() error = %v, want nil", original.id, err)
		return
	}
	second := c.Connection()
	if second == nil {
		t.Errorf("Connection() for consumer %q after reconnecting = nil, want an initialized connection", original.id)
		return
	}
	if second == first || second.Outbound == first.Outbound || second.MessageChan == first.MessageChan || second.AcknowledgedChan == first.AcknowledgedChan || second.HeartbeatChan == first.HeartbeatChan {
		t.Errorf("ConnectConsumer() for consumer %q after DisconnectConsumer() reused resources from %p in %p, want fresh connection resources", original.id, first, second)
	}
	if c.Info != original {
		t.Errorf("Info after reconnecting consumer %q = %+v, want %+v", original.id, c.Info, original)
	}
}

// TestManagerConcurrentLifecycle checks concurrent lifecycle calls.
func TestManagerConcurrentLifecycle(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if err := manager.validate(); err != nil {
		return
	}
	c := openTestConsumer(t, manager, original.id)
	if c == nil {
		return
	}
	const iterations = 32
	operations := []struct {
		name string
		run  func() error
	}{
		{name: "ConnectConsumer", run: func() error { return manager.ConnectConsumer(c) }},
		{name: "DisconnectConsumer", run: func() error { return manager.DisconnectConsumer(c) }},
		{name: "Connection", run: func() error {
			c.Connection()
			return nil
		}},
	}
	start := make(chan struct{})
	var group sync.WaitGroup
	for _, operation := range operations {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for i := 0; i < iterations; i++ {
				if err := operation.run(); err != nil && !(operation.name == "ConnectConsumer" && errors.Is(err, ErrAlreadyConnected)) {
					t.Errorf("concurrent %s() call %d for consumer %q error = %v, want nil or %v for ConnectConsumer", operation.name, i+1, original.id, err, ErrAlreadyConnected)
				}
			}
		}()
	}
	close(start)
	group.Wait()
	if err := manager.DisconnectConsumer(c); err != nil {
		t.Errorf("DisconnectConsumer() after concurrent lifecycle calls for consumer %q error = %v, want nil", original.id, err)
	}
	if err := manager.ConnectConsumer(c); err != nil {
		t.Errorf("ConnectConsumer() after concurrent lifecycle calls for consumer %q error = %v, want nil", original.id, err)
	}
}

// TestManagerRejectsForeignConsumer checks manager ownership.
func TestManagerRejectsForeignConsumer(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Manager, *Consumer) error
	}{
		{name: "ConnectConsumer", run: (*Manager).ConnectConsumer},
		{name: "DisconnectConsumer", run: (*Manager).DisconnectConsumer},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := newTestConsumerQueue(t)
			if ipcPath == "" {
				return
			}
			original := Info{id: "consumer-1", topic: "events", ipcPath: ipcPath}
			owner := newTestManager(t, original)
			other := newTestManager(t, original)
			if owner.validate() != nil || other.validate() != nil {
				return
			}
			c := connectTestConsumer(t, owner, original.id)
			if err := c.Validate(); err != nil {
				return
			}
			connection := c.Connection()
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			connection.Cancel = cancel
			if err := tt.run(other, c); !errors.Is(err, ErrNotFound) {
				t.Errorf("%s(%q) through another manager error = %v, want %v", tt.name, original.id, err, ErrNotFound)
			}
			if got := c.Connection(); got != connection {
				t.Errorf("Connection() for %q after rejected %s = %p, want %p", original.id, tt.name, got, connection)
			}
			if err := ctx.Err(); err != nil {
				t.Errorf("context error for %q after rejected %s = %v, want nil", original.id, tt.name, err)
			}
			if got := owner.activeConsumers.Len(); got != 1 {
				t.Errorf("owner's active connection count after rejected %s(%q) = %d, want 1", tt.name, original.id, got)
			}
			if got := other.activeConsumers.Len(); got != 0 {
				t.Errorf("other manager's active connection count after rejected %s(%q) = %d, want 0", tt.name, original.id, got)
			}
		})
	}
}

// TestManagerDisconnectInvalidMetadata checks cleanup of a registered connection.
func TestManagerDisconnectInvalidMetadata(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if err := manager.validate(); err != nil {
		return
	}
	c := connectTestConsumer(t, manager, original.id)
	if err := c.Validate(); err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c.Connection().Cancel = cancel
	c.Info = Info{}
	if err := manager.DisconnectConsumer(c); err != nil {
		t.Errorf("DisconnectConsumer(%q) after invalidating metadata error = %v, want nil", original.id, err)
	}
	if c.connection != nil {
		t.Errorf("connection for %q after DisconnectConsumer() with invalid metadata = %p, want nil", original.id, c.connection)
	}
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("context error for %q after DisconnectConsumer() with invalid metadata = %v, want %v", original.id, err, context.Canceled)
	}
	if got := manager.activeConsumers.Len(); got != 0 {
		t.Errorf("active connection count after DisconnectConsumer(%q) with invalid metadata = %d, want 0", original.id, got)
	}
	connectTestConsumer(t, manager, original.id)
}
