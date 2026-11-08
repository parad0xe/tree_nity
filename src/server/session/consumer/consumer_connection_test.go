package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

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
	first, err := newConnection(context.Background(), ipcPath)
	if err != nil {
		t.Errorf("newConnection(%q) error = %v, want nil", ipcPath, err)
		return
	}
	cleanupTestConnection(t, first)
	second, err := newConnection(context.Background(), ipcPath)
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
			if tt.connection.Done() == nil {
				t.Errorf("Done() for %s connection to %q = nil, want a cancellation signal", tt.name, ipcPath)
			}
			select {
			case <-tt.connection.Done():
				t.Errorf("Done() for %s connection to %q is closed, want open", tt.name, ipcPath)
			default:
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
	if first.Done() == second.Done() {
		t.Errorf("newConnection(%q) twice shared Done() %v, want distinct signals", ipcPath, first.Done())
	}
}

// TestConnectionDoneWithoutContext checks missing cancellation signals.
func TestConnectionDoneWithoutContext(t *testing.T) {
	tests := []struct {
		name       string
		connection *Connection
	}{
		{name: "nil receiver"},
		{name: "zero value", connection: &Connection{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.connection.Done(); got != nil {
				t.Errorf("Done() for %s = %v, want nil", tt.name, got)
			}
		})
	}
}

// TestConnectionDoneParentCancellation checks cancellation propagation.
func TestConnectionDoneParentCancellation(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	connection, err := newConnection(ctx, ipcPath)
	if err != nil {
		t.Errorf("newConnection(ctx, %q) error = %v, want nil", ipcPath, err)
		return
	}
	cleanupTestConnection(t, connection)
	cancel()
	select {
	case <-connection.Done():
	default:
		t.Errorf("Done() for queue %q after parent cancellation is open, want closed", ipcPath)
	}
}

// TestConnectionClosePreservesParent checks context ownership.
func TestConnectionClosePreservesParent(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	connection, err := newConnection(ctx, ipcPath)
	if err != nil {
		t.Errorf("newConnection(ctx, %q) error = %v, want nil", ipcPath, err)
		return
	}
	cleanupTestConnection(t, connection)
	if err := connection.Close(); err != nil {
		t.Errorf("Close() for queue %q error = %v, want nil", ipcPath, err)
	}
	select {
	case <-connection.Done():
	default:
		t.Errorf("Done() for queue %q after Close() is open, want closed", ipcPath)
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("parent context error after Close() for queue %q = %v, want nil", ipcPath, err)
	}
}

// TestConnectionDoneUnblocksSend checks cancellation without a channel reader.
func TestConnectionDoneUnblocksSend(t *testing.T) {
	tests := []struct {
		name string
		send func(*Connection, <-chan struct{}) string
	}{
		{name: "message", send: func(c *Connection, abort <-chan struct{}) string {
			select {
			case c.MessageChan <- protocol.Message{}:
				return "sent"
			case <-c.Done():
				return "canceled"
			case <-abort:
				return "timed out"
			}
		}},
		{name: "acknowledgement", send: func(c *Connection, abort <-chan struct{}) string {
			select {
			case c.AcknowledgedChan <- 7:
				return "sent"
			case <-c.Done():
				return "canceled"
			case <-abort:
				return "timed out"
			}
		}},
		{name: "heartbeat", send: func(c *Connection, abort <-chan struct{}) string {
			select {
			case c.HeartbeatChan <- struct{}{}:
				return "sent"
			case <-c.Done():
				return "canceled"
			case <-abort:
				return "timed out"
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := newTestConsumerQueue(t)
			if ipcPath == "" {
				return
			}
			connection, err := newConnection(context.Background(), ipcPath)
			if err != nil {
				t.Errorf("newConnection(ctx, %q) error = %v, want nil", ipcPath, err)
				return
			}
			cleanupTestConnection(t, connection)
			limitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			t.Cleanup(cancel)
			started := make(chan struct{})
			result := make(chan string, 1)
			var group sync.WaitGroup
			group.Add(1)
			go func() {
				defer group.Done()
				close(started)
				result <- tt.send(connection, limitCtx.Done())
			}()
			<-started
			if err := connection.Close(); err != nil {
				t.Errorf("Close() while sending %s to queue %q error = %v, want nil", tt.name, ipcPath, err)
			}
			got := <-result
			group.Wait()
			if got != "canceled" {
				t.Errorf("send %s without a reader after Close() for queue %q = %q, want %q", tt.name, ipcPath, got, "canceled")
			}
		})
	}
}

// TestNewConnectionSendsProtocolData checks encoding through a real queue.
func TestNewConnectionSendsProtocolData(t *testing.T) {
	ipcPath := fmt.Sprintf("/consumer.%s.%d", t.Name(), os.Getpid())
	inbound, err := ipc.NewInbound(ipcPath, protocol.Decode)
	if err != nil {
		t.Errorf("NewInbound(%q) error = %v, want nil", ipcPath, err)
		return
	}
	t.Cleanup(func() {
		if err := inbound.Drop(); err != nil {
			t.Errorf("Drop() for inbound %q error = %v, want nil", ipcPath, err)
		}
	})
	connection, err := newConnection(context.Background(), ipcPath)
	if err != nil {
		t.Errorf("newConnection(%q) error = %v, want nil", ipcPath, err)
		return
	}
	cleanupTestConnection(t, connection)
	metadata := protocol.NewClientMetadata("consumer-1", ipcPath)
	payload := protocol.NewConsumerHeartbeatPayload()
	want, err := protocol.NewData(protocol.ConsumerHeartbeat, metadata, payload)
	if err != nil {
		t.Errorf("NewData(%v, %+v, %+v) error = %v, want nil", protocol.ConsumerHeartbeat, metadata, payload, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	if err := connection.Outbound.Send(ctx, want); err != nil {
		t.Errorf("Send(%+v) to %q error = %v, want nil", want, ipcPath, err)
		return
	}
	got, err := inbound.Recv(ctx)
	if err != nil {
		t.Errorf("Recv() from %q after Send(%+v) error = %v, want nil", ipcPath, want, err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Recv() from %q after Send(%+v) = %+v, want %+v", ipcPath, want, got, want)
	}
}

// TestNewConnectionRejectsInvalidData checks propagation of protocol validation errors.
func TestNewConnectionRejectsInvalidData(t *testing.T) {
	ipcPath := newTestConsumerQueue(t)
	if ipcPath == "" {
		return
	}
	connection, err := newConnection(context.Background(), ipcPath)
	if err != nil {
		t.Errorf("newConnection(%q) error = %v, want nil", ipcPath, err)
		return
	}
	cleanupTestConnection(t, connection)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	data := protocol.Data{}
	err = connection.Outbound.Send(ctx, data)
	if !errors.Is(err, ipc.ErrInvalidData) {
		t.Errorf("Send(%+v) to %q error = %v, want %v", data, ipcPath, err, ipc.ErrInvalidData)
	}
	if !errors.Is(err, protocol.ErrMissingMetadata) {
		t.Errorf("Send(%+v) to %q error = %v, want %v", data, ipcPath, err, protocol.ErrMissingMetadata)
	}
}

// TestNewConnectionRejectsCanceledContext checks cancellation before opening IPC.
func TestNewConnectionRejectsCanceledContext(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), time.Unix(0, 0))
	t.Cleanup(cancelExpired)
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{name: "canceled", ctx: canceledCtx, wantErr: context.Canceled},
		{name: "expired", ctx: expiredCtx, wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := "/consumer-missing"
			connection, err := newConnection(tt.ctx, ipcPath)
			if connection != nil {
				cleanupTestConnection(t, connection)
				t.Errorf("newConnection(%s context, %q) = %p, want nil", tt.name, ipcPath, connection)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("newConnection(%s context, %q) error = %v, want %v", tt.name, ipcPath, err, tt.wantErr)
			}
		})
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
			connection, err := newConnection(context.Background(), tt.ipcPath)
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
			connection := &Connection{done: ctx.Done(), cancel: cancel}
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
