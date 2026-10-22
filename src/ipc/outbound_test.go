package ipc

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

const otherTestFrame = "other frame"

var errUnexpectedSend = errors.New("unexpected sendFrame call")

func fixedEncoder(frame []byte) EncodeFunc {
	return func(protocol.Data) ([]byte, error) {
		return frame, nil
	}
}

func sequenceEncoder(frames ...[]byte) EncodeFunc {
	next := 0
	return func(protocol.Data) ([]byte, error) {
		frame := frames[next]
		next++
		return frame, nil
	}
}

func newTestOutbound(t *testing.T, ipcPath string, encode EncodeFunc) *Outbound {
	t.Helper()
	out, err := NewOutbound(ipcPath, encode)
	if err != nil {
		t.Fatalf("NewOutbound(%q) error = %v, want nil", ipcPath, err)
	}
	t.Cleanup(func() {
		if err := out.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	})
	return out
}

func tryNewOutbound(t *testing.T, ipcPath string) error {
	t.Helper()
	out, err := NewOutbound(ipcPath, fixedEncoder([]byte(testFrame)))
	if err == nil {
		t.Cleanup(func() {
			if err := out.Close(); err != nil {
				t.Errorf("Close() error = %v, want nil", err)
			}
		})
	}
	return err
}

func fillInbound(t *testing.T, in *Inbound) {
	t.Helper()
	s := connect(t, in.IPCPath())
	frame := []byte(testFrame)
	for {
		err := s.TimedSend(frame, time.Now())
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return
		}
		if err != nil {
			t.Fatalf("TimedSend(%q) error = %v, want nil", frame, err)
		}
	}
}

func newUnconnectedOutbound(t *testing.T) *Outbound {
	t.Helper()
	return &Outbound{
		ipcPath: "/" + testPrefix(t),
		encode:  fixedEncoder([]byte(testFrame)),
	}
}

func TestNewOutboundConnectsToExistingQueue(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoringDecoder)
	if err := tryNewOutbound(t, in.IPCPath()); err != nil {
		t.Errorf("NewOutbound(%q) error = %v, want nil", in.IPCPath(), err)
	}
}

func TestNewOutboundRejectsMissingQueue(t *testing.T) {
	ipcPath := "/" + testPrefix(t)
	if err := tryNewOutbound(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewOutbound(%q) error = %v, want %v", ipcPath, err, os.ErrNotExist)
	}
}

func TestSendReachesInbound(t *testing.T) {
	rec := &recordingDecoder{}
	in := newTestInbound(t, testPrefix(t), rec.decode)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder([]byte(testFrame)))
	if err := out.Send(testContext(t), protocol.Data{}); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}
	if _, err := in.Recv(testContext(t)); err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if want := []string{testFrame}; !slices.Equal(rec.frames, want) {
		t.Errorf("decoded frames = %q, want %q", rec.frames, want)
	}
}

func TestSendAheadOvertakesWaitingData(t *testing.T) {
	rec := &recordingDecoder{}
	in := newTestInbound(t, testPrefix(t), rec.decode)
	out := newTestOutbound(t, in.IPCPath(), sequenceEncoder([]byte(testFrame), []byte(otherTestFrame)))
	if err := out.Send(testContext(t), protocol.Data{}); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}
	if err := out.SendAhead(testContext(t), protocol.Data{}); err != nil {
		t.Fatalf("SendAhead() error = %v, want nil", err)
	}
	for range 2 {
		if _, err := in.Recv(testContext(t)); err != nil {
			t.Fatalf("Recv() error = %v, want nil", err)
		}
	}
	if want := []string{otherTestFrame, testFrame}; !slices.Equal(rec.frames, want) {
		t.Errorf("decoded frames = %q, want %q", rec.frames, want)
	}
}

func TestSendRejectsInvalidData(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoringDecoder)
	errEncode := errors.New("encode failure")
	out := newTestOutbound(t, in.IPCPath(), func(protocol.Data) ([]byte, error) {
		return nil, errEncode
	})
	err := out.Send(testContext(t), protocol.Data{})
	if !errors.Is(err, ErrInvalidData) {
		t.Errorf("Send() error = %v, want %v", err, ErrInvalidData)
	}
	if !errors.Is(err, errEncode) {
		t.Errorf("Send() error = %v, want %v", err, errEncode)
	}
}

func TestSendRejectsTooLargeFrame(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoringDecoder)
	frame := bytes.Repeat([]byte("a"), 1<<20)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder(frame))
	if err := out.Send(testContext(t), protocol.Data{}); !errors.Is(err, mqueue.ErrFrameTooLarge) {
		t.Errorf("Send(%d bytes) error = %v, want %v", len(frame), err, mqueue.ErrFrameTooLarge)
	}
}

func TestSendRetriesAfterExpiredSlice(t *testing.T) {
	out := newUnconnectedOutbound(t)
	calls := 0
	mockSendFrame := func([]byte, time.Time) error {
		calls++
		switch calls {
		case 1:
			return os.ErrDeadlineExceeded
		case 2:
			return nil
		default:
			return errUnexpectedSend
		}
	}
	if err := out.send(testContext(t), protocol.Data{}, mockSendFrame); err != nil {
		t.Errorf("send() error = %v, want nil", err)
	}
	if calls != 2 {
		t.Errorf("send() called sendFrame %d times, want 2", calls)
	}
}

func TestSendStopsWhenCancelledAfterExpiredSlice(t *testing.T) {
	out := newUnconnectedOutbound(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	mockSendFrame := func([]byte, time.Time) error {
		calls++
		if calls > 1 {
			return errUnexpectedSend
		}
		cancel()
		return os.ErrDeadlineExceeded
	}
	if err := out.send(ctx, protocol.Data{}, mockSendFrame); !errors.Is(err, context.Canceled) {
		t.Errorf("send() error = %v, want %v", err, context.Canceled)
	}
	if calls != 1 {
		t.Errorf("send() called sendFrame %d times, want 1", calls)
	}
}

func TestSendCapsSliceAtContextDeadline(t *testing.T) {
	out := newUnconnectedOutbound(t)
	ctx := fixedDeadlineContext{Context: context.Background(), deadline: time.Now()}
	var got time.Time
	mockSendFrame := func(_ []byte, deadline time.Time) error {
		got = deadline
		return nil
	}
	if err := out.send(ctx, protocol.Data{}, mockSendFrame); err != nil {
		t.Errorf("send() error = %v, want nil", err)
	}
	if !got.Equal(ctx.deadline) {
		t.Errorf("send() passed deadline %v to sendFrame, want the context deadline %v", got, ctx.deadline)
	}
}

func TestSendReturnsAtDeadline(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoringDecoder)
	fillInbound(t, in)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder([]byte(testFrame)))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := startCall(t, "Send()", func() error {
		return out.Send(ctx, protocol.Data{})
	})()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Send() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestSendReturnsImmediatelyWhenAlreadyCancelled(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoringDecoder)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder([]byte(testFrame)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := out.Send(ctx, protocol.Data{}); !errors.Is(err, context.Canceled) {
		t.Errorf("Send() with a cancelled context error = %v, want %v", err, context.Canceled)
	}
}

func TestCloseKeepsQueue(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoringDecoder)
	out, err := NewOutbound(in.IPCPath(), fixedEncoder([]byte(testFrame)))
	if err != nil {
		t.Fatalf("NewOutbound(%q) error = %v, want nil", in.IPCPath(), err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := tryNewOutbound(t, in.IPCPath()); err != nil {
		t.Errorf("NewOutbound(%q) after Close() error = %v, want nil", in.IPCPath(), err)
	}
}
