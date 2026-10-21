package ipc

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"testing"

	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

const otherValidFrame = `{"opcode":1,"emitter":1,"metadata":{},"payload":{}}`

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

func decodeFrame(t *testing.T, frame string) protocol.Data {
	t.Helper()
	data, err := protocol.NewDataFromFrame([]byte(frame))
	if err != nil {
		t.Fatalf("NewDataFromFrame(%q) error = %v, want nil", frame, err)
	}
	return data
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
	out, err := NewOutbound(ipcPath, fixedEncoder([]byte(validFrame)))
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
	in := newTestInbound(t, testPrefix(t), protocol.NewDataFromFrame)
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
	in := newTestInbound(t, testPrefix(t), protocol.NewDataFromFrame)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder([]byte(validFrame)))
	if err := out.Send(protocol.Data{}); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}
	got, err := in.Recv(testContext(t))
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if want := decodeFrame(t, validFrame); !reflect.DeepEqual(got, want) {
		t.Errorf("Recv() = %+v, want %+v", got, want)
	}
}

func TestSendAheadOvertakesWaitingData(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), protocol.NewDataFromFrame)
	out := newTestOutbound(t, in.IPCPath(), sequenceEncoder([]byte(validFrame), []byte(otherValidFrame)))
	if err := out.Send(protocol.Data{}); err != nil {
		t.Fatalf("Send() error = %v, want nil", err)
	}
	if err := out.SendAhead(protocol.Data{}); err != nil {
		t.Fatalf("SendAhead() error = %v, want nil", err)
	}
	got, err := in.Recv(testContext(t))
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if want := decodeFrame(t, otherValidFrame); !reflect.DeepEqual(got, want) {
		t.Errorf("Recv() = %+v, want %+v", got, want)
	}
}

func TestSendRejectsInvalidData(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), protocol.NewDataFromFrame)
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
	in := newTestInbound(t, testPrefix(t), protocol.NewDataFromFrame)
	frame := bytes.Repeat([]byte("a"), 1<<20)
	out := newTestOutbound(t, in.IPCPath(), fixedEncoder(frame))
	if err := out.Send(protocol.Data{}); !errors.Is(err, mqueue.ErrFrameTooLarge) {
		t.Errorf("Send(%d bytes) error = %v, want %v", len(frame), err, mqueue.ErrFrameTooLarge)
	}
}

func TestCloseKeepsQueue(t *testing.T) {
	in := newTestInbound(t, testPrefix(t), protocol.NewDataFromFrame)
	out, err := NewOutbound(in.IPCPath(), fixedEncoder([]byte(validFrame)))
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
