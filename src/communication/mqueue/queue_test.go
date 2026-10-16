package mqueue

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"syscall"
	"testing"
)

func queueName(t *testing.T) string {
	t.Helper()
	return "/" + t.Name()
}

func release(t *testing.T, q *queue) {
	t.Helper()
	if err := q.close(); err != nil && !errors.Is(err, syscall.EBADF) {
		t.Errorf("Close() error = %v, want nil", err)
	}
	if err := q.unlink(); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Unlink() error = %v, want nil", err)
	}
}

func createReader(t *testing.T, name string) *queue {
	t.Helper()
	q, err := open(name, Read, createExclusive, maxMsg, msgSize)
	if err != nil {
		t.Fatalf("Open(%q) error = %v, want nil", name, err)
	}
	t.Cleanup(func() { release(t, q) })
	return q
}

func openQueue(t *testing.T, name string, mode mode, opts option, capacity int64) error {
	t.Helper()
	q, err := open(name, mode, opts, capacity, msgSize)
	if err == nil {
		t.Cleanup(func() { release(t, q) })
	}
	return err
}

func createWriter(t *testing.T, name string) *queue {
	t.Helper()
	q, err := open(name, Write, noOptions, maxMsg, msgSize)
	if err != nil {
		t.Fatalf("Open(%q) error = %v, want nil", name, err)
	}
	t.Cleanup(func() { release(t, q) })
	return q
}

func TestOpenCreatesMissingQueue(t *testing.T) {
	name := queueName(t)
	if err := openQueue(t, name, Read, createExclusive, maxMsg); err != nil {
		t.Errorf("Open(%q) error = %v, want nil", name, err)
	}
}

func TestOpenOpensExistingQueue(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	createWriter(t, name)
}

func TestOpenRejectsExistingQueueWhenExclusive(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	if err := openQueue(t, name, Read, createExclusive, maxMsg); !errors.Is(err, os.ErrExist) {
		t.Errorf("Open(%q) error = %v, want %v", name, err, os.ErrExist)
	}
}

func TestOpenRejectsMissingQueueWithoutCreate(t *testing.T) {
	name := queueName(t)
	if err := openQueue(t, name, Write, noOptions, maxMsg); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open(%q) error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestOpenRejectsZeroCapacity(t *testing.T) {
	name := queueName(t)
	if err := openQueue(t, name, Read, createExclusive, 0); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("Open(%q) error = %v, want %v", name, err, syscall.EINVAL)
	}
}

func TestOpenRejectsInvalidName(t *testing.T) {
	name := "invalid"
	if err := openQueue(t, name, Read, createExclusive, maxMsg); !errors.Is(err, ErrInvalidName) {
		t.Errorf("Open(%q) error = %v, want %v", name, err, ErrInvalidName)
	}
}

func TestOpenReadsActualMessageSize(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	q, err := open(name, Write, noOptions, 1, 1)
	if err != nil {
		t.Fatalf("Open(%q) error = %v, want nil", name, err)
	}
	t.Cleanup(func() { release(t, q) })
	if q.msgSize != msgSize {
		t.Errorf("Open(%q) msgSize = %d, want %d", name, q.msgSize, msgSize)
	}
}

func TestValidateNameAcceptsShortName(t *testing.T) {
	name := "/a"
	if err := validateName(name); err != nil {
		t.Errorf("validateName(%q) error = %v, want nil", name, err)
	}
}

func TestValidateNameAcceptsMaximumLength(t *testing.T) {
	name := "/" + strings.Repeat("a", nameMax)
	if err := validateName(name); err != nil {
		t.Errorf("validateName(%q) error = %v, want nil", name, err)
	}
}

func TestValidateNameRejectsEmptyName(t *testing.T) {
	name := ""
	if err := validateName(name); !errors.Is(err, ErrInvalidName) {
		t.Errorf("validateName(%q) error = %v, want %v", name, err, ErrInvalidName)
	}
}

func TestValidateNameRejectsMissingLeadingSlash(t *testing.T) {
	name := "name"
	if err := validateName(name); !errors.Is(err, ErrInvalidName) {
		t.Errorf("validateName(%q) error = %v, want %v", name, err, ErrInvalidName)
	}
}

func TestValidateNameRejectsSlashOnly(t *testing.T) {
	name := "/"
	if err := validateName(name); !errors.Is(err, ErrInvalidName) {
		t.Errorf("validateName(%q) error = %v, want %v", name, err, ErrInvalidName)
	}
}

func TestValidateNameRejectsInnerSlash(t *testing.T) {
	name := "/a/b"
	if err := validateName(name); !errors.Is(err, ErrInvalidName) {
		t.Errorf("validateName(%q) error = %v, want %v", name, err, ErrInvalidName)
	}
}

func TestValidateNameRejectsTooLongName(t *testing.T) {
	name := "/" + strings.Repeat("a", nameMax+1)
	if err := validateName(name); !errors.Is(err, ErrInvalidName) {
		t.Errorf("validateName(%q) error = %v, want %v", name, err, ErrInvalidName)
	}
}

func TestCloseRejectsClosedQueue(t *testing.T) {
	q := createReader(t, queueName(t))
	if err := q.close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := q.close(); !errors.Is(err, syscall.EBADF) {
		t.Errorf("second Close() error = %v, want %v", err, syscall.EBADF)
	}
}

func TestUnlinkRemovesQueue(t *testing.T) {
	name := queueName(t)
	q := createReader(t, name)
	if err := q.unlink(); err != nil {
		t.Fatalf("Unlink() error = %v, want nil", err)
	}
	if err := openQueue(t, name, Write, noOptions, maxMsg); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open(%q) after Unlink() error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestUnlinkRejectsMissingQueue(t *testing.T) {
	q := createReader(t, queueName(t))
	if err := q.unlink(); err != nil {
		t.Fatalf("Unlink() error = %v, want nil", err)
	}
	if err := q.unlink(); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("second Unlink() error = %v, want %v", err, os.ErrNotExist)
	}
}

func TestDropRemovesQueue(t *testing.T) {
	name := queueName(t)
	q := createReader(t, name)
	if err := q.drop(); err != nil {
		t.Fatalf("Drop() error = %v, want nil", err)
	}
	if err := openQueue(t, name, Write, noOptions, maxMsg); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open(%q) after Drop() error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestRecvReturnsSentMessage(t *testing.T) {
	name := queueName(t)
	reader := createReader(t, name)
	writer := createWriter(t, name)
	message := []byte("key\x00value")
	if err := writer.send(message); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", message, err)
	}
	got, err := reader.recv()
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, message) {
		t.Errorf("Recv() = %q, want %q", got, message)
	}
}

func TestRecvReturnsMaximumSizeMessage(t *testing.T) {
	name := queueName(t)
	reader := createReader(t, name)
	writer := createWriter(t, name)
	message := bytes.Repeat([]byte("a"), msgSize)
	if err := writer.send(message); err != nil {
		t.Fatalf("Send(%d bytes) error = %v, want nil", len(message), err)
	}
	got, err := reader.recv()
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, message) {
		t.Errorf("Recv() = %d bytes, want the %d bytes sent", len(got), len(message))
	}
}

func TestRecvPreservesSendOrder(t *testing.T) {
	name := queueName(t)
	reader := createReader(t, name)
	writer := createWriter(t, name)
	first, second := []byte("first"), []byte("second")
	if err := writer.send(first); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", first, err)
	}
	if err := writer.send(second); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", second, err)
	}
	got, err := reader.recv()
	if err != nil {
		t.Fatalf("first Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, first) {
		t.Errorf("first Recv() = %q, want %q", got, first)
	}
	got, err = reader.recv()
	if err != nil {
		t.Fatalf("second Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, second) {
		t.Errorf("second Recv() = %q, want %q", got, second)
	}
}

func TestSendRejectsEmptyMessage(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	writer := createWriter(t, name)
	message := []byte{}
	if err := writer.send(message); !errors.Is(err, ErrEmptyMessage) {
		t.Errorf("Send(%q) error = %v, want %v", message, err, ErrEmptyMessage)
	}
}

func TestSendRejectsTooLargeMessage(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	writer := createWriter(t, name)
	message := bytes.Repeat([]byte("a"), msgSize+1)
	if err := writer.send(message); !errors.Is(err, ErrMessageTooLarge) {
		t.Errorf("Send(%d bytes) error = %v, want %v", len(message), err, ErrMessageTooLarge)
	}
}

func TestRecvRejectsClosedQueue(t *testing.T) {
	q := createReader(t, queueName(t))
	if err := q.close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if _, err := q.recv(); !errors.Is(err, syscall.EBADF) {
		t.Errorf("Recv() after Close() error = %v, want %v", err, syscall.EBADF)
	}
}
