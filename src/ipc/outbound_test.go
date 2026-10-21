package ipc

import (
	"bytes"
	"errors"
	"os"
	"slices"
	"testing"

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
	if err := out.Send(protocol.Data{}); err != nil {
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
	if err := out.Send(protocol.Data{}); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}
	if err := out.SendAhead(protocol.Data{}); err != nil {
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
	err := out.Send(protocol.Data{})
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
	if err := out.Send(protocol.Data{}); !errors.Is(err, mqueue.ErrFrameTooLarge) {
		t.Errorf("Send(%d bytes) error = %v, want %v", len(frame), err, mqueue.ErrFrameTooLarge)
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
