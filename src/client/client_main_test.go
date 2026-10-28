package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"tree_nity/src/exitcode"
)

// pipeReader reads what is written to a pipe, line by line.
type pipeReader struct {
	pipe   *os.File
	reader *bufio.Reader
}

// newPipeReader returns a pipeReader that reads from r.
func newPipeReader(r *os.File) *pipeReader {
	return &pipeReader{
		pipe:   r,
		reader: bufio.NewReader(r),
	}
}

// waitFor reads lines until one contains want. It fails the test when no such
// line arrives within testTimeout.
func (p *pipeReader) waitFor(t *testing.T, want string) {
	t.Helper()
	if err := p.pipe.SetReadDeadline(time.Now().Add(testTimeout)); err != nil {
		t.Fatalf("SetReadDeadline() error = %v, want nil", err)
	}
	for {
		line, err := p.reader.ReadString('\n')
		if strings.Contains(line, want) {
			return
		}
		if err != nil {
			t.Fatalf("ReadString() error = %v, want a line containing %q within %v", err, want, testTimeout)
		}
	}
}

// captureStderr sends os.Stderr to a pipe until the test ends, so that the test
// can read what the code under test writes to it.
func captureStderr(t *testing.T) *pipeReader {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe() error = %v, want nil", err)
	}
	stderr := os.Stderr
	os.Stderr = w
	t.Cleanup(func() {
		os.Stderr = stderr
		if err := w.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
		if err := r.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	})
	return newPipeReader(r)
}

// setArgs sets os.Args to args until the test ends.
func setArgs(t *testing.T, args ...string) {
	t.Helper()
	previous := os.Args
	os.Args = args
	t.Cleanup(func() {
		os.Args = previous
	})
}

func TestParseRejectsMissingIPCIdentifier(t *testing.T) {
	args := []string{}

	if _, _, err := parse(args); !errors.Is(err, errMissingIPCIdentifier) {
		t.Errorf("parse(%q) error = %v, want %v", args, err, errMissingIPCIdentifier)
	}
}

func TestParseRejectsMissingCommand(t *testing.T) {
	args := []string{"/server"}

	if _, _, err := parse(args); !errors.Is(err, errMissingCommand) {
		t.Errorf("parse(%q) error = %v, want %v", args, err, errMissingCommand)
	}
}

func TestParseRejectsUnknownCommand(t *testing.T) {
	args := []string{"/server", "unknown"}

	if _, _, err := parse(args); !errors.Is(err, errUnknownCommand) {
		t.Errorf("parse(%q) error = %v, want %v", args, err, errUnknownCommand)
	}
}

func TestRunRejectsInvalidArguments(t *testing.T) {
	setArgs(t, "client")
	stderr := captureStderr(t)

	code := run(context.Background())

	if code != exitcode.General {
		t.Errorf("run() without arguments code = %d, want %d", code, exitcode.General)
	}
	stderr.waitFor(t, errMissingIPCIdentifier.Error())
	stderr.waitFor(t, fmt.Sprintf(usage, os.Args[0]))
	ipcPath := clientIPCPath(os.Getpid())
	if err := tryConnect(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after run() error = %v, want %v", ipcPath, err, os.ErrNotExist)
	}
}
