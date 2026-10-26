package main

import (
	"bytes"
	"errors"
	"os"
	"syscall"
	"testing"

	"tree_nity/src/exitcode"
)

// errWrite is the error returned by failingWriter.
var errWrite = errors.New("write failed")

// failingWriter is an io.Writer whose writes always fail.
type failingWriter struct{}

// Write fails with errWrite.
func (failingWriter) Write([]byte) (int, error) {
	return 0, errWrite
}

func TestRunPrintsIPCPath(t *testing.T) {
	var stdout bytes.Buffer

	code := run(cancelledContext(), &stdout)

	if code != exitcode.Success {
		t.Errorf("run() code = %d, want %d", code, exitcode.Success)
	}
	ipcPath := serverIPCPath(os.Getpid())
	if got, want := stdout.String(), ipcPath+"\n"; got != want {
		t.Errorf("run() stdout = %q, want %q", got, want)
	}
	if err := tryConnect(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after run() error = %v, want %v", ipcPath, err, os.ErrNotExist)
	}
}

func TestRunFailsWhenStdoutFails(t *testing.T) {
	stderr := captureStderr(t)

	code := run(cancelledContext(), failingWriter{})

	if code != exitcode.General {
		t.Errorf("run() with a failing stdout code = %d, want %d", code, exitcode.General)
	}
	stderr.waitFor(t, errPrintIPCPath.Error())
	ipcPath := serverIPCPath(os.Getpid())
	if err := tryConnect(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after run() error = %v, want %v", ipcPath, err, os.ErrNotExist)
	}
}

func TestRunFailsWhenQueueExists(t *testing.T) {
	newTestApplication(t)
	stderr := captureStderr(t)
	var stdout bytes.Buffer

	code := run(cancelledContext(), &stdout)

	if code != exitcode.General {
		t.Errorf("run() with an existing queue code = %d, want %d", code, exitcode.General)
	}
	if got := stdout.String(); got != "" {
		t.Errorf("run() with an existing queue stdout = %q, want empty", got)
	}
	stderr.waitFor(t, syscall.EEXIST.Error())
	// run must not remove a queue it did not create.
	ipcPath := serverIPCPath(os.Getpid())
	if err := tryConnect(t, ipcPath); err != nil {
		t.Errorf("NewSender(%q) after run() error = %v, want nil", ipcPath, err)
	}
}
