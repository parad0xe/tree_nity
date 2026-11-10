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

func newTestEncodedConnection(t *testing.T, parent context.Context, ipcPath string, encode ipc.EncodeFunc) *Connection {
	t.Helper()
	outbound, err := ipc.NewOutbound(ipcPath, encode)
	if err != nil {
		t.Errorf("NewOutbound(%q) error = %v, want nil", ipcPath, err)
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	connection := &Connection{outbound: outbound, done: ctx.Done(), cancel: cancel}
	cleanupTestConnection(t, connection)
	return connection
}

func fillTestConsumerQueue(t *testing.T, ipcPath string) bool {
	t.Helper()
	filler, err := mqueue.NewSender(ipcPath)
	if err != nil {
		t.Errorf("NewSender(%q) error = %v, want nil", ipcPath, err)
		return false
	}
	t.Cleanup(func() {
		if err := filler.Close(); err != nil {
			t.Errorf("Close() for queue filler %q error = %v, want nil", ipcPath, err)
		}
	})
	for {
		err := filler.TimedSend([]byte("fill"), time.Unix(1, 0))
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return true
		}
		if err != nil {
			t.Errorf("TimedSend() while filling queue %q error = %v, want nil or %v", ipcPath, err, os.ErrDeadlineExceeded)
			return false
		}
	}
}

// TestConnectionClosed checks the shutdown signal without waiting.
func TestConnectionClosed(t *testing.T) {
	open := make(chan struct{})
	stopped := make(chan struct{})
	close(stopped)
	tests := []struct {
		name string
		done <-chan struct{}
		want bool
	}{
		{name: "no signal"},
		{name: "open", done: open},
		{name: "stopped", done: stopped, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connection := &Connection{done: tt.done}
			if got := connection.closed(); got != tt.want {
				t.Errorf("closed() for %s signal = %t, want %t", tt.name, got, tt.want)
			}
		})
	}
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
			if tt.connection.outbound == nil {
				t.Errorf("newConnection(%q).outbound for %s connection = nil, want an initialized outbound", ipcPath, tt.name)
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
	metadata := protocol.NewClientMetadata(ipcPath)
	payload := protocol.NewConsumerHeartbeatPayload("consumer-1")
	want, err := protocol.NewData(protocol.ConsumerHeartbeat, metadata, payload)
	if err != nil {
		t.Errorf("NewData(%v, %+v, %+v) error = %v, want nil", protocol.ConsumerHeartbeat, metadata, payload, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	if err := connection.Send(ctx, want); err != nil {
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
	err = connection.Send(ctx, data)
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

// TestConnectionValidate checks an allocated connection.
func TestConnectionValidate(t *testing.T) {
	connection := &Connection{}
	if err := connection.validate(); err != nil {
		t.Errorf("validate() on %+v error = %v, want nil", connection, err)
	}
}

// TestConnectionValidateRejectsNil checks the missing-connection error.
func TestConnectionValidateRejectsNil(t *testing.T) {
	var connection *Connection
	if err := connection.validate(); !errors.Is(err, ErrConnectionUninitialized) {
		t.Errorf("validate() on nil connection error = %v, want %v", err, ErrConnectionUninitialized)
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
	connection := &Connection{outbound: outbound}
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

// TestConnectionSendUninitialized checks missing connection resources.
func TestConnectionSendUninitialized(t *testing.T) {
	tests := []struct {
		name       string
		connection *Connection
		send       func(*Connection, context.Context, protocol.Data) error
	}{
		{name: "Send nil", send: (*Connection).Send},
		{name: "Send zero", connection: &Connection{}, send: (*Connection).Send},
		{name: "SendAhead nil", send: (*Connection).SendAhead},
		{name: "SendAhead zero", connection: &Connection{}, send: (*Connection).SendAhead},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.send(tt.connection, context.Background(), protocol.Data{}); !errors.Is(err, ErrConnectionUninitialized) {
				t.Errorf("%s with empty data error = %v, want %v", tt.name, err, ErrConnectionUninitialized)
			}
		})
	}
}

// TestConnectionSendCanceledCaller checks per-send cancellation.
func TestConnectionSendCanceledCaller(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	expiredCtx, cancelExpired := context.WithDeadline(context.Background(), time.Unix(0, 0))
	t.Cleanup(cancelExpired)
	tests := []struct {
		name    string
		ctx     context.Context
		send    func(*Connection, context.Context, protocol.Data) error
		wantErr error
	}{
		{name: "Send canceled", ctx: canceledCtx, send: (*Connection).Send, wantErr: context.Canceled},
		{name: "Send expired", ctx: expiredCtx, send: (*Connection).Send, wantErr: context.DeadlineExceeded},
		{name: "SendAhead canceled", ctx: canceledCtx, send: (*Connection).SendAhead, wantErr: context.Canceled},
		{name: "SendAhead expired", ctx: expiredCtx, send: (*Connection).SendAhead, wantErr: context.DeadlineExceeded},
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
			if err := tt.send(connection, tt.ctx, protocol.Data{}); !errors.Is(err, tt.wantErr) {
				t.Errorf("%s for queue %q error = %v, want %v", tt.name, ipcPath, err, tt.wantErr)
			}
			select {
			case <-connection.Done():
				t.Errorf("Done() for queue %q after %s is closed, want open", ipcPath, tt.name)
			default:
			}
		})
	}
}

// TestConnectionCloseCancelsBlockedSend checks shutdown with a full queue.
func TestConnectionCloseCancelsBlockedSend(t *testing.T) {
	tests := []struct {
		name string
		send func(*Connection, context.Context, protocol.Data) error
	}{
		{name: "Send", send: (*Connection).Send},
		{name: "SendAhead", send: (*Connection).SendAhead},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := newTestConsumerQueue(t)
			if ipcPath == "" {
				return
			}
			if !fillTestConsumerQueue(t, ipcPath) {
				return
			}
			entered := make(chan struct{})
			connection := newTestEncodedConnection(t, context.Background(), ipcPath, func(protocol.Data) ([]byte, error) {
				close(entered)
				return []byte("frame"), nil
			})
			if err := connection.validate(); err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			t.Cleanup(cancel)
			result := make(chan error, 1)
			go func() { result <- tt.send(connection, ctx, protocol.Data{}) }()
			select {
			case <-entered:
			case err := <-result:
				t.Errorf("%s for full queue %q returned %v before encoding, want an active send", tt.name, ipcPath, err)
				return
			}
			if err := connection.Close(); err != nil {
				t.Errorf("Close() during %s for full queue %q error = %v, want nil", tt.name, ipcPath, err)
			}
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Errorf("%s for full queue %q after Close() error = %v, want %v", tt.name, ipcPath, err, context.Canceled)
			}
		})
	}
}

// TestConnectionParentCancellationStopsSend checks active send cancellation.
func TestConnectionParentCancellationStopsSend(t *testing.T) {
	tests := []struct {
		name string
		send func(*Connection, context.Context, protocol.Data) error
	}{
		{name: "Send", send: (*Connection).Send},
		{name: "SendAhead", send: (*Connection).SendAhead},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipcPath := newTestConsumerQueue(t)
			if ipcPath == "" || !fillTestConsumerQueue(t, ipcPath) {
				return
			}
			parent, stop := context.WithCancel(context.Background())
			t.Cleanup(stop)
			entered := make(chan struct{})
			connection := newTestEncodedConnection(t, parent, ipcPath, func(protocol.Data) ([]byte, error) {
				close(entered)
				return []byte("frame"), nil
			})
			if err := connection.validate(); err != nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			t.Cleanup(cancel)
			result := make(chan error, 1)
			go func() { result <- tt.send(connection, ctx, protocol.Data{}) }()
			select {
			case <-entered:
			case err := <-result:
				t.Errorf("%s() for full queue %q returned %v before encoding, want an active send", tt.name, ipcPath, err)
				return
			}
			stop()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Errorf("%s() for queue %q after parent cancellation error = %v, want %v", tt.name, ipcPath, err, context.Canceled)
			}
		})
	}
}

// TestConnectionClosedSendKeepsOtherQueueEmpty checks stale connection rejection.
func TestConnectionClosedSendKeepsOtherQueueEmpty(t *testing.T) {
	tests := []struct {
		name string
		send func(*Connection, context.Context, protocol.Data) error
	}{
		{name: "Send", send: (*Connection).Send},
		{name: "SendAhead", send: (*Connection).SendAhead},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldPath := newTestConsumerQueue(t, "old")
			if oldPath == "" {
				return
			}
			previous, err := newConnection(context.Background(), oldPath)
			if err != nil {
				t.Errorf("newConnection(ctx, %q) error = %v, want nil", oldPath, err)
				return
			}
			cleanupTestConnection(t, previous)
			newPath := fmt.Sprintf("/consumer.%s.%d.new", strings.ReplaceAll(t.Name(), "/", "."), os.Getpid())
			receiver, err := mqueue.NewReceiver(newPath)
			if err != nil {
				t.Errorf("NewReceiver(%q) error = %v, want nil", newPath, err)
				return
			}
			t.Cleanup(func() {
				if err := receiver.Drop(); err != nil {
					t.Errorf("Drop() for queue %q error = %v, want nil", newPath, err)
				}
			})
			if err := previous.Close(); err != nil {
				t.Errorf("Close() for old queue %q error = %v, want nil", oldPath, err)
				return
			}
			current, err := newConnection(context.Background(), newPath)
			if err != nil {
				t.Errorf("newConnection(ctx, %q) error = %v, want nil", newPath, err)
				return
			}
			cleanupTestConnection(t, current)
			data, err := protocol.NewData(protocol.ConsumerHeartbeat, protocol.NewClientMetadata(oldPath), protocol.NewConsumerHeartbeatPayload("old-consumer"))
			if err != nil {
				t.Errorf("NewData() for consumer %q at %q error = %v, want nil", "old-consumer", oldPath, err)
				return
			}
			if err := tt.send(previous, context.Background(), data); !errors.Is(err, context.Canceled) {
				t.Errorf("%s() for closed queue %q after opening %q error = %v, want %v", tt.name, oldPath, newPath, err, context.Canceled)
			}
			if frame, err := receiver.TimedRecv(time.Unix(1, 0)); !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Errorf("TimedRecv() from %q after stale %s = %q, error = %v, want no frame and %v", newPath, tt.name, frame, err, os.ErrDeadlineExceeded)
			}
		})
	}
}

// TestConnectionSendAheadOvertakesQueuedFrames checks send priority.
func TestConnectionSendAheadOvertakesQueuedFrames(t *testing.T) {
	ipcPath := fmt.Sprintf("/consumer.%s.%d", t.Name(), os.Getpid())
	receiver, err := mqueue.NewReceiver(ipcPath)
	if err != nil {
		t.Errorf("NewReceiver(%q) error = %v, want nil", ipcPath, err)
		return
	}
	t.Cleanup(func() {
		if err := receiver.Drop(); err != nil {
			t.Errorf("Drop() for queue %q error = %v, want nil", ipcPath, err)
		}
	})
	connection, err := newConnection(context.Background(), ipcPath)
	if err != nil {
		t.Errorf("newConnection(ctx, %q) error = %v, want nil", ipcPath, err)
		return
	}
	cleanupTestConnection(t, connection)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	frames := make([]protocol.Data, 3)
	for i := range frames {
		clientID := fmt.Sprintf("consumer-%d", i)
		frames[i], err = protocol.NewData(protocol.ConsumerHeartbeat, protocol.NewClientMetadata(ipcPath), protocol.NewConsumerHeartbeatPayload(clientID))
		if err != nil {
			t.Errorf("NewData() for %q at %q error = %v, want nil", clientID, ipcPath, err)
			return
		}
		if i == len(frames)-1 {
			err = connection.SendAhead(ctx, frames[i])
		} else {
			err = connection.Send(ctx, frames[i])
		}
		if err != nil {
			t.Errorf("send frame %d to %q error = %v, want nil", i, ipcPath, err)
			return
		}
	}
	for position, index := range []int{2, 0, 1} {
		frame, err := receiver.TimedRecv(time.Now().Add(time.Second))
		if err != nil {
			t.Errorf("TimedRecv() for frame %d from %q error = %v, want nil", position, ipcPath, err)
			return
		}
		got, err := protocol.Decode(frame)
		if err != nil || !reflect.DeepEqual(got, frames[index]) {
			t.Errorf("decoded frame %d from %q = %+v, error = %v, want %+v and nil", position, ipcPath, got, err, frames[index])
		}
	}
}
