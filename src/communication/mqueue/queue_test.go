package mqueue_test

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"tree_nity/src/communication/mqueue"
)

const (
	maxMsg  = 10
	msgSize = 128

	noOptions       mqueue.Option = 0
	createExclusive               = mqueue.Create | mqueue.Exclusive
)

func queueName(t *testing.T) string {
	t.Helper()
	return "/" + t.Name()
}

func release(t *testing.T, q *mqueue.Queue) {
	t.Helper()
	if err := q.Close(); err != nil && !errors.Is(err, syscall.EBADF) {
		t.Errorf("Close() error = %v, want nil", err)
	}
	if err := q.Unlink(); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Unlink() error = %v, want nil", err)
	}
}

func createQueue(t *testing.T, name string) *mqueue.Queue {
	t.Helper()
	q, err := mqueue.Open(name, mqueue.Read, createExclusive, maxMsg, msgSize)
	if err != nil {
		t.Fatalf("Open(%q) error = %v, want nil", name, err)
	}
	t.Cleanup(func() { release(t, q) })
	return q
}

func openQueue(t *testing.T, name string, mode mqueue.Mode, opts mqueue.Option, capacity int64) error {
	t.Helper()
	q, err := mqueue.Open(name, mode, opts, capacity, msgSize)
	if err == nil {
		t.Cleanup(func() { release(t, q) })
	}
	return err
}

func TestOpenCreatesMissingQueue(t *testing.T) {
	name := queueName(t)
	if err := openQueue(t, name, mqueue.Read, createExclusive, maxMsg); err != nil {
		t.Errorf("Open(%q) error = %v, want nil", name, err)
	}
}

func TestOpenOpensExistingQueue(t *testing.T) {
	name := queueName(t)
	createQueue(t, name)
	if err := openQueue(t, name, mqueue.Write, noOptions, maxMsg); err != nil {
		t.Errorf("Open(%q) error = %v, want nil", name, err)
	}
}

func TestOpenRejectsExistingQueueWhenExclusive(t *testing.T) {
	name := queueName(t)
	createQueue(t, name)
	if err := openQueue(t, name, mqueue.Read, createExclusive, maxMsg); !errors.Is(err, os.ErrExist) {
		t.Errorf("Open(%q) error = %v, want %v", name, err, os.ErrExist)
	}
}

func TestOpenRejectsMissingQueueWithoutCreate(t *testing.T) {
	name := queueName(t)
	if err := openQueue(t, name, mqueue.Write, noOptions, maxMsg); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open(%q) error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestOpenRejectsZeroCapacity(t *testing.T) {
	name := queueName(t)
	if err := openQueue(t, name, mqueue.Read, createExclusive, 0); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("Open(%q) error = %v, want %v", name, err, syscall.EINVAL)
	}
}

func TestOpenRejectsInvalidName(t *testing.T) {
	name := "invalid"
	if err := openQueue(t, name, mqueue.Read, createExclusive, maxMsg); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("Open(%q) error = %v, want %v", name, err, syscall.EINVAL)
	}
}

func TestCloseRejectsClosedQueue(t *testing.T) {
	q := createQueue(t, queueName(t))
	if err := q.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := q.Close(); !errors.Is(err, syscall.EBADF) {
		t.Errorf("second Close() error = %v, want %v", err, syscall.EBADF)
	}
}

func TestUnlinkRemovesQueue(t *testing.T) {
	name := queueName(t)
	q := createQueue(t, name)
	if err := q.Unlink(); err != nil {
		t.Fatalf("Unlink() error = %v, want nil", err)
	}
	if err := openQueue(t, name, mqueue.Write, noOptions, maxMsg); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open(%q) after Unlink() error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestUnlinkRejectsMissingQueue(t *testing.T) {
	q := createQueue(t, queueName(t))
	if err := q.Unlink(); err != nil {
		t.Fatalf("Unlink() error = %v, want nil", err)
	}
	if err := q.Unlink(); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("second Unlink() error = %v, want %v", err, os.ErrNotExist)
	}
}

func TestDropRemovesQueue(t *testing.T) {
	name := queueName(t)
	q := createQueue(t, name)
	if err := q.Drop(); err != nil {
		t.Fatalf("Drop() error = %v, want nil", err)
	}
	if err := openQueue(t, name, mqueue.Write, noOptions, maxMsg); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open(%q) after Drop() error = %v, want %v", name, err, os.ErrNotExist)
	}
}
