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

func TestNewOutboundConnectsToExistingQueue(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
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
	rec := &frameRecorder{}
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
	rec := &frameRecorder{}
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
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
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
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
	frame := bytes.Repeat([]byte("a"), 1<<20)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder(frame))
	if err := out.Send(testContext(t), protocol.Data{}); !errors.Is(err, mqueue.ErrFrameTooLarge) {
		t.Errorf("Send(%d bytes) error = %v, want %v", len(frame), err, mqueue.ErrFrameTooLarge)
	}
}

func TestSendWaitsWhileQueueIsFull(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
	fillInbound(t, in)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder([]byte(testFrame)))
	ctx := testContext(t)
	wait := startCall(t, "Send()", func() error {
		return out.Send(ctx, protocol.Data{})
	})
	time.Sleep(sendSlice + 50*time.Millisecond)
	if _, err := in.Recv(ctx); err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if err := wait(); err != nil {
		t.Errorf("Send() after Recv() on a full queue error = %v, want nil", err)
	}
}

func TestSendReturnsWhenCancelled(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
	fillInbound(t, in)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder([]byte(testFrame)))
	ctx, cancel := context.WithCancel(context.Background())
	wait := startCall(t, "Send()", func() error {
		return out.Send(ctx, protocol.Data{})
	})
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := wait(); !errors.Is(err, context.Canceled) {
		t.Errorf("Send() after cancel error = %v, want %v", err, context.Canceled)
	}
}

func TestSendReturnsAtDeadline(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
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
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder([]byte(testFrame)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := out.Send(ctx, protocol.Data{}); !errors.Is(err, context.Canceled) {
		t.Errorf("Send() with a cancelled context error = %v, want %v", err, context.Canceled)
	}
}

func TestCloseKeepsQueue(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), ignoreFrame)
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
