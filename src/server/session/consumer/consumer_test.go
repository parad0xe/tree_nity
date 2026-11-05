package consumer

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

// TestNewConsumer checks metadata ownership and the supplied connection.
func TestNewConsumer(t *testing.T) {
	info := newInfo("consumer-1", "events", 7, "events/", "/consumer-1")
	wantInfo := *info
	connection := &Connection{}
	c := newConsumer(info, connection)
	if c == nil {
		t.Errorf("newConsumer(%+v, %p) = nil, want a consumer", wantInfo, connection)
		return
	}
	if c.Info != wantInfo {
		t.Errorf("newConsumer(%+v, %p).Info = %+v, want %+v", wantInfo, connection, c.Info, wantInfo)
	}
	if c.Connection != connection {
		t.Errorf("newConsumer(%+v, %p).Connection = %p, want %p", wantInfo, connection, c.Connection, connection)
	}

	*info = *newInfo("other", "other-topic", 99, "other/", "/other")
	if c.Info != wantInfo {
		t.Errorf("consumer.Info after changing constructor input to %+v = %+v, want %+v", *info, c.Info, wantInfo)
	}
}

// TestConsumerCloseWithoutConnection checks repeated closure of missing resources.
func TestConsumerCloseWithoutConnection(t *testing.T) {
	tests := []struct {
		name     string
		consumer *Consumer
	}{
		{name: "nil receiver"},
		{name: "zero value", consumer: &Consumer{}},
		{name: "empty connection", consumer: &Consumer{Connection: &Connection{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 2; i++ {
				if err := tt.consumer.Close(); err != nil {
					t.Errorf("Close() call %d for %s error = %v, want nil", i+1, tt.name, err)
				}
			}
		})
	}
}

// TestConsumerCloseCancelsContext checks cleanup even when metadata is invalid.
func TestConsumerCloseCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := &Consumer{Connection: &Connection{Cancel: cancel}}
	if err := c.Close(); err != nil {
		t.Errorf("Close() with zero metadata error = %v, want nil", err)
	}
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("Close() with zero metadata: context error = %v, want %v", err, context.Canceled)
	}
}

// TestConsumerCloseAtFunctionReturn checks deferred closure of a manager-created connection.
func TestConsumerCloseAtFunctionReturn(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	original := Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: ipcPath}
	manager := newTestManager(t, original)
	if manager == nil {
		return
	}
	connection := func() *Connection {
		c := openTestConsumer(t, manager, original.id)
		if c == nil {
			return nil
		}
		defer func() {
			if err := c.Close(); err != nil {
				t.Errorf("deferred Close() for consumer %q error = %v, want nil", original.id, err)
			}
		}()
		return c.Connection
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
		t.Errorf("Send(%+v) to %q after deferred Close() error = %v, want %v", data, ipcPath, err, syscall.EBADF)
	}
	if err := (&Consumer{Connection: connection}).Close(); err != nil {
		t.Errorf("repeated Close() for consumer %q error = %v, want nil", original.id, err)
	}
	sender, err := mqueue.NewSender(ipcPath)
	if err != nil {
		t.Errorf("NewSender(%q) after deferred Close() error = %v, want nil because the receiver still owns the queue", ipcPath, err)
		return
	}
	if err := sender.Close(); err != nil {
		t.Errorf("Close() for replacement sender %q error = %v, want nil", ipcPath, err)
	}
}

// TestConsumerValidate checks a consumer with valid metadata and a connection.
func TestConsumerValidate(t *testing.T) {
	c := &Consumer{
		Info:       *newInfo("consumer-1", "events", 7, "events/", "/consumer-1"),
		Connection: &Connection{},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate() on %+v error = %v, want nil", c, err)
	}
}

// TestConsumerValidateRejectsInvalidState checks receiver, metadata, and connection errors.
func TestConsumerValidateRejectsInvalidState(t *testing.T) {
	validInfo := *newInfo("consumer-1", "events", 7, "events/", "/consumer-1")
	tests := []struct {
		name     string
		consumer *Consumer
		wantErr  error
	}{
		{name: "nil receiver", wantErr: ErrUninitialized},
		{name: "zero value", consumer: &Consumer{}, wantErr: id.ErrInvalidID},
		{
			name:     "invalid metadata",
			consumer: &Consumer{Info: Info{id: "invalid/id"}, Connection: &Connection{}},
			wantErr:  id.ErrInvalidID,
		},
		{name: "nil connection", consumer: &Consumer{Info: validInfo}, wantErr: ErrConnectionUninitialized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.consumer.Validate(); !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() on %+v error = %v, want %v", tt.consumer, err, tt.wantErr)
			}
		})
	}
}
