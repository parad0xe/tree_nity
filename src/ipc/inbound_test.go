package ipc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

const (
	testFrame   = "frame"
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

func ignoreFrame([]byte) (protocol.Data, error) {
	return protocol.Data{}, nil
}

type frameRecorder struct {
	frames []string
}

func (r *frameRecorder) decode(frame []byte) (protocol.Data, error) {
	r.frames = append(r.frames, string(frame))
	return protocol.Data{}, nil
}

func newTestInbound(t *testing.T, prefix string, decode DecodeFunc) *Inbound {
	t.Helper()
	in, err := NewInbound(prefix, decode)
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
	in, err := NewInbound(prefix, ignoreFrame)
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

func startCall(t *testing.T, name string, call func() error) func() error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		errCh <- call()
	}()
	return func() error {
		t.Helper()
		select {
		case err := <-errCh:
			return err
		case <-time.After(testTimeout):
			t.Fatalf("%s did not return within %v", name, testTimeout)
			return nil
		}
	}
}

func TestNewInboundCreatesQueue(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
	if err := tryConnect(t, in.IPCPath()); err != nil {
		t.Errorf("NewSender(%q) error = %v, want nil", in.IPCPath(), err)
	}
}

func TestNewInboundNamesQueueAfterPID(t *testing.T) {
	prefix := "/" + testPrefix(t) + "."
	in := newTestInbound(t, prefix, ignoreFrame)
	want := fmt.Sprintf("/%s.%d", testPrefix(t), os.Getpid())
	if got := in.IPCPath(); got != want {
		t.Errorf("NewInbound(%q) IPCPath() = %q, want %q", prefix, got, want)
	}
}

func TestNewInboundRejectsExistingQueue(t *testing.T) {
	prefix := testPrefix(t)
	newTestInbound(t, prefix, ignoreFrame)
	if err := tryNewInbound(t, prefix); !errors.Is(err, os.ErrExist) {
		t.Errorf("NewInbound(%q) error = %v, want %v", prefix, err, os.ErrExist)
	}
}

func TestRecvDecodesFrame(t *testing.T) {
	rec := &frameRecorder{}
	in := newTestInbound(t, testPrefix(t), rec.decode)
	frame := []byte(testFrame)
	if err := connect(t, in.IPCPath()).Send(frame); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", frame, err)
	}
	if _, err := in.Recv(testContext(t)); err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if want := []string{testFrame}; !slices.Equal(rec.frames, want) {
		t.Errorf("decoded frames = %q, want %q", rec.frames, want)
	}
}

func TestRecvRejectsInvalidFrame(t *testing.T) {
	errDecode := errors.New("decode failure")
	in := newTestInbound(t, testPrefix(t), func([]byte) (protocol.Data, error) {
		return protocol.Data{}, errDecode
	})
	frame := []byte(testFrame)
	if err := connect(t, in.IPCPath()).Send(frame); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", frame, err)
	}
	_, err := in.Recv(testContext(t))
	if !errors.Is(err, ErrInvalidFrame) {
		t.Errorf("Recv() error = %v, want %v", err, ErrInvalidFrame)
	}
	if !errors.Is(err, errDecode) {
		t.Errorf("Recv() error = %v, want %v", err, errDecode)
	}
}

func TestRecvReturnsWhenCancelled(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
	ctx, cancel := context.WithCancel(context.Background())
	wait := startCall(t, "Recv()", func() error {
		_, err := in.Recv(ctx)
		return err
	})
	cancel()
	if err := wait(); !errors.Is(err, context.Canceled) {
		t.Errorf("Recv() after cancel error = %v, want %v", err, context.Canceled)
	}
}

func TestRecvReturnsAtDeadline(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := startCall(t, "Recv()", func() error {
		_, err := in.Recv(ctx)
		return err
	})()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Recv() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestRecvReturnsImmediatelyWhenAlreadyCancelled(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := in.Recv(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Recv() with a cancelled context error = %v, want %v", err, context.Canceled)
	}
}

func TestRecvSkipsStaleWakeFrame(t *testing.T) {
	rec := &frameRecorder{}
	in := newTestInbound(t, testPrefix(t), rec.decode)
	s := connect(t, in.IPCPath())
	if err := s.SendAhead(wakeFrame); err != nil {
		t.Fatalf("SendAhead(%q) error = %v, want nil", wakeFrame, err)
	}
	frame := []byte(testFrame)
	if err := s.Send(frame); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", frame, err)
	}
	if _, err := in.Recv(testContext(t)); err != nil {
		t.Fatalf("Recv() after a stale wake frame error = %v, want nil", err)
	}
	if want := []string{testFrame}; !slices.Equal(rec.frames, want) {
		t.Errorf("decoded frames = %q, want %q", rec.frames, want)
	}
}

func TestDropRemovesQueue(t *testing.T) {
	prefix := testPrefix(t)
	in, err := NewInbound(prefix, ignoreFrame)
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
