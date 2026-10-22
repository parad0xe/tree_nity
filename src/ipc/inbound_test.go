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

var errUnexpectedRecv = errors.New("unexpected recvFrame call")

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

func ignoringDecoder([]byte) (protocol.Data, error) {
	return protocol.Data{}, nil
}

type recordingDecoder struct {
	frames []string
}

func (r *recordingDecoder) decode(frame []byte) (protocol.Data, error) {
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
	in, err := NewInbound(prefix, ignoringDecoder)
	if err == nil {
		t.Cleanup(func() {
			if err := in.Drop(); err != nil {
				t.Errorf("Drop() error = %v, want nil", err)
			}
		})
	}
	return err
}

func newUnconnectedInbound(t *testing.T, decode DecodeFunc) *Inbound {
	t.Helper()
	return &Inbound{
		ipcPath: "/" + testPrefix(t),
		decode:  decode,
	}
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

type fixedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c fixedDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

func TestNewInboundCreatesQueue(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoringDecoder)
	if err := tryConnect(t, in.IPCPath()); err != nil {
		t.Errorf("NewSender(%q) error = %v, want nil", in.IPCPath(), err)
	}
}

func TestNewInboundNamesQueueAfterPID(t *testing.T) {
	prefix := "/" + testPrefix(t) + "."
	in := newTestInbound(t, prefix, ignoringDecoder)
	want := fmt.Sprintf("/%s.%d", testPrefix(t), os.Getpid())
	if got := in.IPCPath(); got != want {
		t.Errorf("NewInbound(%q) IPCPath() = %q, want %q", prefix, got, want)
	}
}

func TestNewInboundRejectsExistingQueue(t *testing.T) {
	prefix := testPrefix(t)
	newTestInbound(t, prefix, ignoringDecoder)
	if err := tryNewInbound(t, prefix); !errors.Is(err, os.ErrExist) {
		t.Errorf("NewInbound(%q) error = %v, want %v", prefix, err, os.ErrExist)
	}
}

func TestNewInboundRejectsNilDecoder(t *testing.T) {
	prefix := testPrefix(t)
	if _, err := NewInbound(prefix, nil); !errors.Is(err, ErrNilDecoder) {
		t.Errorf("NewInbound(%q, nil) error = %v, want %v", prefix, err, ErrNilDecoder)
	}
	ipcPath := fmt.Sprintf("/%s.%d", prefix, os.Getpid())
	if err := tryConnect(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after NewInbound(%q, nil) error = %v, want %v", ipcPath, prefix, err, os.ErrNotExist)
	}
}

func TestRecvDecodesFrame(t *testing.T) {
	rec := &recordingDecoder{}
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

func TestRecvRetriesAfterExpiredSlice(t *testing.T) {
	rec := &recordingDecoder{}
	in := newUnconnectedInbound(t, rec.decode)
	calls := 0
	mockRecvFrame := func(time.Time) ([]byte, error) {
		calls++
		switch calls {
		case 1:
			return nil, os.ErrDeadlineExceeded
		case 2:
			return []byte(testFrame), nil
		default:
			return nil, errUnexpectedRecv
		}
	}
	if _, err := in.recv(testContext(t), mockRecvFrame); err != nil {
		t.Errorf("recv() error = %v, want nil", err)
	}
	if calls != 2 {
		t.Errorf("recv() called recvFrame %d times, want 2", calls)
	}
	if want := []string{testFrame}; !slices.Equal(rec.frames, want) {
		t.Errorf("decoded frames = %q, want %q", rec.frames, want)
	}
}

func TestRecvStopsWhenCancelledAfterExpiredSlice(t *testing.T) {
	in := newUnconnectedInbound(t, ignoringDecoder)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	mockRecvFrame := func(time.Time) ([]byte, error) {
		calls++
		if calls > 1 {
			return nil, errUnexpectedRecv
		}
		cancel()
		return nil, os.ErrDeadlineExceeded
	}
	if _, err := in.recv(ctx, mockRecvFrame); !errors.Is(err, context.Canceled) {
		t.Errorf("recv() error = %v, want %v", err, context.Canceled)
	}
	if calls != 1 {
		t.Errorf("recv() called recvFrame %d times, want 1", calls)
	}
}

func TestRecvCapsSliceAtContextDeadline(t *testing.T) {
	in := newUnconnectedInbound(t, ignoringDecoder)
	ctx := fixedDeadlineContext{Context: context.Background(), deadline: time.Now()}
	var got time.Time
	mockRecvFrame := func(deadline time.Time) ([]byte, error) {
		got = deadline
		return []byte(testFrame), nil
	}
	if _, err := in.recv(ctx, mockRecvFrame); err != nil {
		t.Errorf("recv() error = %v, want nil", err)
	}
	if !got.Equal(ctx.deadline) {
		t.Errorf("recv() passed deadline %v to recvFrame, want the context deadline %v", got, ctx.deadline)
	}
}

func TestRecvReturnsAtDeadline(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoringDecoder)
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
	in := newTestInbound(t, testPrefix(t), ignoringDecoder)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := in.Recv(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Recv() with a cancelled context error = %v, want %v", err, context.Canceled)
	}
}

func TestDropRemovesQueue(t *testing.T) {
	prefix := testPrefix(t)
	in, err := NewInbound(prefix, ignoringDecoder)
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
