package ipc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

const (
	validFrame  = `{"opcode":0,"emitter":1,"metadata":{},"payload":{}}`
	testTimeout = 5 * time.Second
)

func testPrefix(t *testing.T) string {
	t.Helper()
	return "test." + t.Name()
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

func newTestInbound(t *testing.T, prefix string) *Inbound {
	t.Helper()
	in, err := NewInbound(prefix)
	if err != nil {
		t.Fatalf("NewInbound(%q) error = %v, want nil", prefix, err)
	}
	t.Cleanup(func() {
		if err := in.Drop(); err != nil {
			t.Errorf("Drop() error = %v, want nil", err)
		}
	})
	return in
}

func tryNewInbound(t *testing.T, prefix string) error {
	t.Helper()
	in, err := NewInbound(prefix)
	if err == nil {
		t.Cleanup(func() {
			if err := in.Drop(); err != nil {
				t.Errorf("Drop() error = %v, want nil", err)
			}
		})
	}
	return err
}

func connect(t *testing.T, name string) *mqueue.Sender {
	t.Helper()
	s, err := mqueue.NewSender(name)
	if err != nil {
		t.Fatalf("NewSender(%q) error = %v, want nil", name, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	})
	return s
}

func tryConnect(t *testing.T, name string) error {
	t.Helper()
	s, err := mqueue.NewSender(name)
	if err == nil {
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("Close() error = %v, want nil", err)
			}
		})
	}
	return err
}

func TestNewInboundCreatesQueue(t *testing.T) {
	in := newTestInbound(t, testPrefix(t))
	if err := tryConnect(t, in.IPCPath()); err != nil {
		t.Errorf("NewSender(%q) error = %v, want nil", in.IPCPath(), err)
	}
}

func TestNewInboundNamesQueueAfterPID(t *testing.T) {
	prefix := "/" + testPrefix(t) + "."
	in := newTestInbound(t, prefix)
	want := fmt.Sprintf("/%s.%d", testPrefix(t), os.Getpid())
	if got := in.IPCPath(); got != want {
		t.Errorf("NewInbound(%q) IPCPath() = %q, want %q", prefix, got, want)
	}
}

func TestNewInboundRejectsExistingQueue(t *testing.T) {
	prefix := testPrefix(t)
	newTestInbound(t, prefix)
	if err := tryNewInbound(t, prefix); !errors.Is(err, os.ErrExist) {
		t.Errorf("NewInbound(%q) error = %v, want %v", prefix, err, os.ErrExist)
	}
}

func TestRecvReturnsData(t *testing.T) {
	in := newTestInbound(t, testPrefix(t))
	frame := []byte(validFrame)
	if err := connect(t, in.IPCPath()).Send(frame); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", frame, err)
	}
	want, err := protocol.NewDataFromFrame(frame)
	if err != nil {
		t.Fatalf("NewDataFromFrame(%q) error = %v, want nil", frame, err)
	}
	got, err := in.Recv(testContext(t))
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Recv() = %+v, want %+v", got, want)
	}
}

func TestRecvRejectsInvalidFrame(t *testing.T) {
	in := newTestInbound(t, testPrefix(t))
	frame := []byte(`{}`)
	if err := connect(t, in.IPCPath()).Send(frame); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", frame, err)
	}
	_, err := in.Recv(testContext(t))
	if !errors.Is(err, ErrInvalidFrame) {
		t.Errorf("Recv() error = %v, want %v", err, ErrInvalidFrame)
	}
	if !errors.Is(err, protocol.ErrMissingOpcode) {
		t.Errorf("Recv() error = %v, want %v", err, protocol.ErrMissingOpcode)
	}
}

func TestRecvReturnsWhenCancelled(t *testing.T) {
	in := newTestInbound(t, testPrefix(t))
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := in.Recv(ctx)
		errCh <- err
	}()
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Recv() after cancel error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(testTimeout):
		t.Fatalf("Recv() did not return within %v after cancel", testTimeout)
	}
}

func TestRecvReturnsAtDeadline(t *testing.T) {
	in := newTestInbound(t, testPrefix(t))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := in.Recv(ctx)
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Recv() error = %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(testTimeout):
		t.Fatalf("Recv() did not return within %v after its deadline", testTimeout)
	}
}

func TestRecvReturnsImmediatelyWhenAlreadyCancelled(t *testing.T) {
	in := newTestInbound(t, testPrefix(t))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := in.Recv(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Recv() with a cancelled context error = %v, want %v", err, context.Canceled)
	}
}

func TestRecvSkipsStaleWakeFrame(t *testing.T) {
	in := newTestInbound(t, testPrefix(t))
	s := connect(t, in.IPCPath())
	if err := s.SendAhead(wakeFrame); err != nil {
		t.Fatalf("SendAhead(%q) error = %v, want nil", wakeFrame, err)
	}
	frame := []byte(validFrame)
	if err := s.Send(frame); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", frame, err)
	}
	if _, err := in.Recv(testContext(t)); err != nil {
		t.Errorf("Recv() after a stale wake frame error = %v, want nil", err)
	}
}

func TestDropRemovesQueue(t *testing.T) {
	prefix := testPrefix(t)
	in, err := NewInbound(prefix)
	if err != nil {
		t.Fatalf("NewInbound(%q) error = %v, want nil", prefix, err)
	}
	if err := in.Drop(); err != nil {
		t.Fatalf("Drop() error = %v, want nil", err)
	}
	if err := tryConnect(t, in.IPCPath()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after Drop() error = %v, want %v", in.IPCPath(), err, os.ErrNotExist)
	}
}
