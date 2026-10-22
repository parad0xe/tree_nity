package mqueue

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"
)

func newSender(t *testing.T, name string) *Sender {
	t.Helper()
	s, err := NewSender(name)
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

func tryNewSender(t *testing.T, name string) error {
	t.Helper()
	s, err := NewSender(name)
	if err == nil {
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("Close() error = %v, want nil", err)
			}
		})
	}
	return err
}

func TestNewSenderRejectsMissingQueue(t *testing.T) {
	name := queueName(t)
	if err := tryNewSender(t, name); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestSendReachesReceiver(t *testing.T) {
	name := queueName(t)
	receiver := newReceiver(t, name)
	sender := newSender(t, name)
	frame := []byte("frame")
	if err := sender.Send(frame); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", frame, err)
	}
	got, err := receiver.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, frame) {
		t.Errorf("Recv() = %q, want %q", got, frame)
	}
}

func TestSendAheadOvertakesWaitingFrames(t *testing.T) {
	name := queueName(t)
	receiver := newReceiver(t, name)
	sender := newSender(t, name)
	normal, ahead := []byte("normal"), []byte("ahead")
	if err := sender.Send(normal); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", normal, err)
	}
	if err := sender.SendAhead(ahead); err != nil {
		t.Fatalf("SendAhead(%q) error = %v, want nil", ahead, err)
	}
	got, err := receiver.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, ahead) {
		t.Errorf("Recv() = %q, want %q", got, ahead)
	}
}

func TestTimedSendReachesReceiver(t *testing.T) {
	name := queueName(t)
	receiver := newReceiver(t, name)
	sender := newSender(t, name)
	frame := []byte("frame")
	if err := sender.TimedSend(frame, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("TimedSend(%q) error = %v, want nil", frame, err)
	}
	got, err := receiver.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, frame) {
		t.Errorf("Recv() = %q, want %q", got, frame)
	}
}

func TestTimedSendAheadOvertakesWaitingFrames(t *testing.T) {
	name := queueName(t)
	receiver := newReceiver(t, name)
	sender := newSender(t, name)
	deadline := time.Now().Add(time.Second)
	normal, ahead := []byte("normal"), []byte("ahead")
	if err := sender.TimedSend(normal, deadline); err != nil {
		t.Fatalf("TimedSend(%q) error = %v, want nil", normal, err)
	}
	if err := sender.TimedSendAhead(ahead, deadline); err != nil {
		t.Fatalf("TimedSendAhead(%q) error = %v, want nil", ahead, err)
	}
	got, err := receiver.Recv()
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, ahead) {
		t.Errorf("Recv() = %q, want %q", got, ahead)
	}
}

func TestSenderCloseKeepsQueue(t *testing.T) {
	name := queueName(t)
	newReceiver(t, name)
	s, err := NewSender(name)
	if err != nil {
		t.Fatalf("NewSender(%q) error = %v, want nil", name, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := tryNewSender(t, name); err != nil {
		t.Errorf("NewSender(%q) after Close() error = %v, want nil", name, err)
	}
}
