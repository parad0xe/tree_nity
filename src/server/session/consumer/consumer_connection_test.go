package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

func newTestConsumerQueue(t *testing.T, suffix ...string) string {
	t.Helper()
	ipcPath := fmt.Sprintf("/consumer.%s.%d.%s", strings.ReplaceAll(t.Name(), "/", "."), os.Getpid(), strings.Join(suffix, "."))
	receiver, err := mqueue.NewReceiver(ipcPath)
	if err != nil {
		t.Errorf("NewReceiver(%q) error = %v, want nil", ipcPath, err)
		return ""
	}
	t.Cleanup(func() {
		if err := receiver.Drop(); err != nil {
			t.Errorf("Drop() for queue %q error = %v, want nil", ipcPath, err)
		}
	})
	return ipcPath
}

func cleanupTestConnection(t *testing.T, connection *Connection) {
	t.Helper()
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Errorf("Close() for connection %p error = %v, want nil", connection, err)
		}
	})
}

// TestNewConnection checks channel initialization and independence between connections.
func TestNewConnection(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	first, err := newConnection(ipcPath)
	if err != nil {
		t.Errorf("newConnection(%q) error = %v, want nil", ipcPath, err)
		return
	}
	cleanupTestConnection(t, first)
	second, err := newConnection(ipcPath)
	if err != nil {
		t.Errorf("second newConnection(%q) error = %v, want nil", ipcPath, err)
		return
	}
	cleanupTestConnection(t, second)
	if first == nil || second == nil {
		t.Errorf("newConnection(%q) twice = (%p, %p), want two connections", ipcPath, first, second)
		return
	}
	if first == second {
		t.Errorf("newConnection(%q) twice returned the same pointer %p, want distinct connections", ipcPath, first)
	}
	tests := []struct {
		name       string
		connection *Connection
	}{
		{name: "first", connection: first},
		{name: "second", connection: second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.connection.Outbound == nil {
				t.Errorf("newConnection(%q).Outbound for %s connection = nil, want an initialized outbound", ipcPath, tt.name)
			}
			if tt.connection.MessageChan == nil {
				t.Errorf("newConnection(%q).MessageChan for %s connection = nil, want an initialized channel", ipcPath, tt.name)
			}
			if tt.connection.AcknowledgedChan == nil {
				t.Errorf("newConnection(%q).AcknowledgedChan for %s connection = nil, want an initialized channel", ipcPath, tt.name)
			}
			if tt.connection.HeartbeatChan == nil {
				t.Errorf("newConnection(%q).HeartbeatChan for %s connection = nil, want an initialized channel", ipcPath, tt.name)
			}
		})
	}
	if first.MessageChan == second.MessageChan {
		t.Errorf("newConnection(%q) twice shared MessageChan %v, want distinct channels", ipcPath, first.MessageChan)
	}
	if first.AcknowledgedChan == second.AcknowledgedChan {
		t.Errorf("newConnection(%q) twice shared AcknowledgedChan %v, want distinct channels", ipcPath, first.AcknowledgedChan)
	}
	if first.HeartbeatChan == second.HeartbeatChan {
		t.Errorf("newConnection(%q) twice shared HeartbeatChan %v, want distinct channels", ipcPath, first.HeartbeatChan)
	}
}

// TestNewConnectionPropagatesQueueError checks invalid and missing queue errors.
func TestNewConnectionPropagatesQueueError(t *testing.T) {
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
			connection, err := newConnection(tt.ipcPath)
			if connection != nil {
				cleanupTestConnection(t, connection)
				t.Errorf("newConnection(%q) connection = %p, want nil on initialization failure", tt.ipcPath, connection)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("newConnection(%q) error = %v, want %v", tt.ipcPath, err, tt.wantErr)
			}
		})
	}
}

// TestConnectionValidate checks the missing-connection error.
func TestConnectionValidate(t *testing.T) {
	tests := []struct {
		name       string
		connection *Connection
		wantErr    error
	}{
		{name: "nil receiver", wantErr: ErrConnectionUninitialized},
		{name: "zero value", connection: &Connection{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.connection.validate(); !errors.Is(err, tt.wantErr) {
				t.Errorf("validate() on %+v error = %v, want %v", tt.connection, err, tt.wantErr)
			}
		})
	}
}

// TestConnectionCloseWithoutCancel checks that closing missing resources is safe.
func TestConnectionCloseWithoutCancel(t *testing.T) {
	tests := []struct {
		name       string
		connection *Connection
	}{
		{name: "nil receiver"},
		{name: "zero value", connection: &Connection{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 2; i++ {
				if err := tt.connection.Close(); err != nil {
					t.Errorf("Close() call %d for %s error = %v, want nil", i+1, tt.name, err)
				}
			}
		})
	}
}

// TestConnectionCloseCancelsContext checks cancellation and repeated closure.
func TestConnectionCloseCancelsContext(t *testing.T) {
	tests := []struct {
		name            string
		closeCount      int
		alreadyCanceled bool
	}{
		{name: "once", closeCount: 1},
		{name: "twice", closeCount: 2},
		{name: "already canceled", closeCount: 1, alreadyCanceled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			otherCtx, cancelOther := context.WithCancel(context.Background())
			t.Cleanup(cancelOther)
			connection := &Connection{Cancel: cancel}
			if tt.alreadyCanceled {
				cancel()
			}
			for i := 0; i < tt.closeCount; i++ {
				if err := connection.Close(); err != nil {
					t.Errorf("Close() call %d with alreadyCanceled=%t error = %v, want nil", i+1, tt.alreadyCanceled, err)
				}
			}
			if err := ctx.Err(); !errors.Is(err, context.Canceled) {
				t.Errorf("Close() %d times with alreadyCanceled=%t: context error = %v, want %v", tt.closeCount, tt.alreadyCanceled, err, context.Canceled)
			}
			if err := otherCtx.Err(); err != nil {
				t.Errorf("Close() %d times with alreadyCanceled=%t: unrelated context error = %v, want nil", tt.closeCount, tt.alreadyCanceled, err)
			}
		})
	}
}

// TestConnectionCloseOutbound checks descriptor closure, repeated closure, and queue ownership.
func TestConnectionCloseOutbound(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	outbound, err := ipc.NewOutbound(ipcPath, func(protocol.Data) ([]byte, error) {
		return []byte("consumer test frame"), nil
	})
	if err != nil {
		t.Errorf("NewOutbound(%q) error = %v, want nil", ipcPath, err)
		return
	}
	connection := &Connection{Outbound: outbound}
	cleanupTestConnection(t, connection)
	for i := 0; i < 2; i++ {
		if err := connection.Close(); err != nil {
			t.Errorf("Close() call %d for outbound %q error = %v, want nil", i+1, ipcPath, err)
		}
	}
	if err := outbound.Send(context.Background(), protocol.Data{}); !errors.Is(err, syscall.EBADF) {
		t.Errorf("Send() after Close() for outbound %q error = %v, want %v", ipcPath, err, syscall.EBADF)
	}
	sender, err := mqueue.NewSender(ipcPath)
	if err != nil {
		t.Errorf("NewSender(%q) after Close() error = %v, want nil because the receiver still owns the queue", ipcPath, err)
		return
	}
	if err := sender.Close(); err != nil {
		t.Errorf("Close() for replacement sender %q error = %v, want nil", ipcPath, err)
	}
}
