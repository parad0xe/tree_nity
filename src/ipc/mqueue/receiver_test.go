package mqueue

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"
)

func newReceiver(t *testing.T, name string) *Receiver {
	t.Helper()
	r, err := NewReceiver(name)
	if err != nil {
		t.Fatalf("NewReceiver(%q) error = %v, want nil", name, err)
	}
	t.Cleanup(func() {
		if err := r.Drop(); err != nil {
			t.Errorf("Drop() error = %v, want nil", err)
		}
	})
	return r
}

func tryNewReceiver(t *testing.T, name string) error {
	t.Helper()
	r, err := NewReceiver(name)
	if err == nil {
		t.Cleanup(func() {
			if err := r.Drop(); err != nil {
				t.Errorf("Drop() error = %v, want nil", err)
			}
		})
	}
	return err
}

func TestNewReceiverRejectsExistingQueue(t *testing.T) {
	name := queueName(t)
	newReceiver(t, name)
	if err := tryNewReceiver(t, name); !errors.Is(err, os.ErrExist) {
		t.Errorf("NewReceiver(%q) error = %v, want %v", name, err, os.ErrExist)
	}
}

func TestReceiverDropRemovesQueue(t *testing.T) {
	name := queueName(t)
	r, err := NewReceiver(name)
	if err != nil {
		t.Fatalf("NewReceiver(%q) error = %v, want nil", name, err)
	}
	if err := r.Drop(); err != nil {
		t.Fatalf("Drop() error = %v, want nil", err)
	}
	if err := tryNewSender(t, name); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after Drop() error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestReceiverUnlinkRemovesQueue(t *testing.T) {
	name := queueName(t)
	r := newReceiver(t, name)
	if err := r.Unlink(); err != nil {
		t.Fatalf("Unlink() error = %v, want nil", err)
	}
	if err := tryNewSender(t, name); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after Unlink() error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestReceiverUnlinkKeepsQueueOpen(t *testing.T) {
	name := queueName(t)
	r := newReceiver(t, name)
	s := newSender(t, name)
	if err := r.Unlink(); err != nil {
		t.Fatalf("Unlink() error = %v, want nil", err)
	}
	frame := []byte("frame")
	if err := s.Send(frame); err != nil {
		t.Fatalf("Send(%q) after Unlink() error = %v, want nil", frame, err)
	}
	got, err := r.TimedRecv(time.Now().Add(testTimeout))
	if err != nil {
		t.Fatalf("TimedRecv() after Unlink() error = %v, want nil", err)
	}
	if !bytes.Equal(got, frame) {
		t.Errorf("TimedRecv() after Unlink() = %q, want %q", got, frame)
	}
}
