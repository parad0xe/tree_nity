package mqueue

import (
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

func createQueue(t *testing.T, name string) *queue {
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

func TestOpenCreatesMissingQueue(t *testing.T) {
	name := queueName(t)
	if err := openQueue(t, name, Read, createExclusive, maxMsg); err != nil {
		t.Errorf("Open(%q) error = %v, want nil", name, err)
	}
}

func TestOpenOpensExistingQueue(t *testing.T) {
	name := queueName(t)
	createQueue(t, name)
	if err := openQueue(t, name, Write, noOptions, maxMsg); err != nil {
		t.Errorf("Open(%q) error = %v, want nil", name, err)
	}
}

func TestOpenRejectsExistingQueueWhenExclusive(t *testing.T) {
	name := queueName(t)
	createQueue(t, name)
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
	q := createQueue(t, queueName(t))
	if err := q.close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := q.close(); !errors.Is(err, syscall.EBADF) {
		t.Errorf("second Close() error = %v, want %v", err, syscall.EBADF)
	}
}

func TestUnlinkRemovesQueue(t *testing.T) {
	name := queueName(t)
	q := createQueue(t, name)
	if err := q.unlink(); err != nil {
		t.Fatalf("Unlink() error = %v, want nil", err)
	}
	if err := openQueue(t, name, Write, noOptions, maxMsg); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open(%q) after Unlink() error = %v, want %v", name, err, os.ErrNotExist)
	}
}

func TestUnlinkRejectsMissingQueue(t *testing.T) {
	q := createQueue(t, queueName(t))
	if err := q.unlink(); err != nil {
		t.Fatalf("Unlink() error = %v, want nil", err)
	}
	if err := q.unlink(); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("second Unlink() error = %v, want %v", err, os.ErrNotExist)
	}
}

func TestDropRemovesQueue(t *testing.T) {
	name := queueName(t)
	q := createQueue(t, name)
	if err := q.drop(); err != nil {
		t.Fatalf("Drop() error = %v, want nil", err)
	}
	if err := openQueue(t, name, Write, noOptions, maxMsg); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open(%q) after Drop() error = %v, want %v", name, err, os.ErrNotExist)
	}
}
