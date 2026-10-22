package consumer

import (
	"context"
	"errors"
	"testing"
)

// TestNewConnection checks channel initialization and independence between connections.
func TestNewConnection(t *testing.T) {
	ipcPath := "/consumer-test"
	first, second := newConnection(ipcPath), newConnection(ipcPath)
	if first == nil || second == nil {
		t.Errorf("newConnection(%q) twice = (%p, %p), want two connections", ipcPath, first, second)
		return
	}
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
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
			tt.connection.Close()
			tt.connection.Close()
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
				connection.Close()
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
