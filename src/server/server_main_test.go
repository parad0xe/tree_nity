package main

import (
	"errors"
	"os"
	"syscall"
	"testing"

	"tree_nity/src/exitcode"
)

// redirectStdout sets os.Stdout to f until the test ends.
func redirectStdout(t *testing.T, f *os.File) {
	t.Helper()
	stdout := os.Stdout
	os.Stdout = f
	t.Cleanup(func() {
		os.Stdout = stdout
	})
}

// captureStdout sends os.Stdout to a temporary file until the test ends, and
// returns the name of that file.
func captureStdout(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := f.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	})
	redirectStdout(t, f)
	return f.Name()
}

// breakStdout sets os.Stdout to a closed file until the test ends, so that
// every write to it fails.
func breakStdout(t *testing.T) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v, want nil", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	redirectStdout(t, f)
}

// readFile returns the content of the file name.
func readFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v, want nil", name, err)
	}
	return string(data)
}

func TestRunPrintsIPCPath(t *testing.T) {
	stdoutName := captureStdout(t)

	code := run(cancelledContext())

	if code != exitcode.Success {
		t.Errorf("run() code = %d, want %d", code, exitcode.Success)
	}
	ipcPath := serverIPCPath(os.Getpid())
	if got, want := readFile(t, stdoutName), ipcPath+"\n"; got != want {
		t.Errorf("run() stdout = %q, want %q", got, want)
	}
	if err := tryConnect(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after run() error = %v, want %v", ipcPath, err, os.ErrNotExist)
	}
}

func TestRunFailsWhenStdoutFails(t *testing.T) {
	stderr := captureStderr(t)
	breakStdout(t)

	code := run(cancelledContext())

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
	stdoutName := captureStdout(t)

	code := run(cancelledContext())

	if code != exitcode.General {
		t.Errorf("run() with an existing queue code = %d, want %d", code, exitcode.General)
	}
	if got := readFile(t, stdoutName); got != "" {
		t.Errorf("run() with an existing queue stdout = %q, want empty", got)
	}
	stderr.waitFor(t, syscall.EEXIST.Error())
	// run must not remove a queue it did not create.
	ipcPath := serverIPCPath(os.Getpid())
	if err := tryConnect(t, ipcPath); err != nil {
		t.Errorf("NewSender(%q) after run() error = %v, want nil", ipcPath, err)
	}
}
