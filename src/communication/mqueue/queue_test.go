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
	q, err := create(name, frameSize)
	if err != nil {
		t.Fatalf("Create(%q) error = %v, want nil", name, err)
	}
	t.Cleanup(func() { release(t, q) })
	return q
}

func createWriter(t *testing.T, name string) *queue {
	t.Helper()
	q, err := connect(name)
	if err != nil {
		t.Fatalf("Connect(%q) error = %v, want nil", name, err)
	}
	t.Cleanup(func() { release(t, q) })
	return q
}

func tryCreateReader(t *testing.T, name string, size int) error {
	t.Helper()
	q, err := create(name, size)
	if err == nil {
		t.Cleanup(func() { release(t, q) })
	}
	return err
}

func tryCreateWriter(t *testing.T, name string) error {
	t.Helper()
	q, err := connect(name)
	if err == nil {
		t.Cleanup(func() { release(t, q) })
	}
	return err
}

func TestCreateCreatesMissingQueue(t *testing.T) {
	name := queueName(t)
	if err := tryCreateReader(t, name, frameSize); err != nil {
		t.Errorf("Create(%q) error = %v, want nil", name, err)
	}
}

func TestCreateRejectsExistingQueue(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	if err := tryCreateReader(t, name, frameSize); !errors.Is(err, os.ErrExist) {
		t.Errorf("Create(%q) error = %v, want %v", name, err, os.ErrExist)
	}
}

func TestCreateRejectsZeroFrameSize(t *testing.T) {
	name := queueName(t)
	if err := tryCreateReader(t, name, 0); !errors.Is(err, syscall.EINVAL) {
		t.Errorf("Create(%q) error = %v, want %v", name, err, syscall.EINVAL)
	}
}

func TestCreateRejectsInvalidName(t *testing.T) {
	name := "invalid"
	if err := tryCreateReader(t, name, frameSize); !errors.Is(err, ErrInvalidName) {
		t.Errorf("Create(%q) error = %v, want %v", name, err, ErrInvalidName)
	}
}

func TestConnectOpensExistingQueue(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	if err := tryCreateWriter(t, name); err != nil {
		t.Errorf("Connect(%q) error = %v, want nil", name, err)
	}
}

func TestConnectRejectsMissingQueue(t *testing.T) {
	name := queueName(t)
	if err := tryCreateWriter(t, name); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Connect(%q) error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestConnectReadsActualAttributes(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	writer := createWriter(t, name)
	if writer.frameSize != frameSize {
		t.Errorf("Connect(%q) frameSize = %d, want %d", name, writer.frameSize, frameSize)
	}
	if writer.maxFrame != maxFrame {
		t.Errorf("Connect(%q) maxFrame = %d, want %d", name, writer.maxFrame, maxFrame)
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
	if err := tryCreateWriter(t, name); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Connect(%q) after Unlink() error = %v, want %v", name, err, os.ErrNotExist)
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
	if err := tryCreateWriter(t, name); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Connect(%q) after Drop() error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestRecvReturnsSentFrame(t *testing.T) {
	name := queueName(t)
	reader := createReader(t, name)
	writer := createWriter(t, name)
	frame := []byte("key\x00value")
	if err := writer.send(frame); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", frame, err)
	}
	got, err := reader.recv()
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, frame) {
		t.Errorf("Recv() = %q, want %q", got, frame)
	}
}

func TestRecvReturnsMaximumSizeFrame(t *testing.T) {
	name := queueName(t)
	reader := createReader(t, name)
	writer := createWriter(t, name)
	frame := bytes.Repeat([]byte("a"), frameSize)
	if err := writer.send(frame); err != nil {
		t.Fatalf("Send(%d bytes) error = %v, want nil", len(frame), err)
	}
	got, err := reader.recv()
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	if !bytes.Equal(got, frame) {
		t.Errorf("Recv() = %d bytes, want the %d bytes sent", len(got), len(frame))
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

func TestSendRejectsEmptyFrame(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	writer := createWriter(t, name)
	frame := []byte{}
	if err := writer.send(frame); !errors.Is(err, ErrEmptyFrame) {
		t.Errorf("Send(%q) error = %v, want %v", frame, err, ErrEmptyFrame)
	}
}

func TestSendRejectsTooLargeFrame(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	writer := createWriter(t, name)
	frame := bytes.Repeat([]byte("a"), frameSize+1)
	if err := writer.send(frame); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("Send(%d bytes) error = %v, want %v", len(frame), err, ErrFrameTooLarge)
	}
}

func TestSendRejectsReaderQueue(t *testing.T) {
	reader := createReader(t, queueName(t))
	frame := []byte("frame")
	if err := reader.send(frame); !errors.Is(err, syscall.EBADF) {
		t.Errorf("Send(%q) on reader error = %v, want %v", frame, err, syscall.EBADF)
	}
}

func TestRecvRejectsWriterQueue(t *testing.T) {
	name := queueName(t)
	createReader(t, name)
	writer := createWriter(t, name)
	if _, err := writer.recv(); !errors.Is(err, syscall.EBADF) {
		t.Errorf("Recv() on writer error = %v, want %v", err, syscall.EBADF)
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
