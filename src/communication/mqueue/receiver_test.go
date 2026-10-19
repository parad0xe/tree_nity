package mqueue

import (
	"errors"
	"os"
	"testing"
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
