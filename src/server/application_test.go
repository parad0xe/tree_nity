package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/lifecycle"
)

const (
	// testTimeout is the longest a test waits for an event before it fails.
	testTimeout = 5 * time.Second
	// validFrame decodes to protocol data whose opcode the server does not handle.
	validFrame = `{"opcode":0,"emitter":1,"metadata":{},"payload":{}}`
	// invalidFrame does not decode to protocol data.
	invalidFrame = "not json"
)

// call is a function running in its own goroutine. Its context is cancelled,
// and the goroutine waited for, when the test ends.
type call struct {
	name     string
	cancel   context.CancelFunc
	done     chan lifecycle.ExitResult
	result   lifecycle.ExitResult
	returned bool
}

// startCall runs f in its own goroutine, with a context derived from parent.
func startCall(t *testing.T, parent context.Context, name string, f func(context.Context) (int, error)) *call {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	c := &call{
		name:   name,
		cancel: cancel,
		done:   make(chan lifecycle.ExitResult, 1),
	}
	go func() {
		code, err := f(ctx)
		c.done <- lifecycle.NewExitResult(code, err)
	}()
	t.Cleanup(func() {
		c.cancel()
		c.wait(t)
	})
	return c
}

// wait returns the result of the call. It fails the test when the call does
// not return within testTimeout.
func (c *call) wait(t *testing.T) lifecycle.ExitResult {
	t.Helper()
	if !c.returned {
		select {
		case c.result = <-c.done:
			c.returned = true
		case <-time.After(testTimeout):
			t.Fatalf("%s did not return within %v", c.name, testTimeout)
		}
	}
	return c.result
}

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

// serverIPCPath returns the inbound queue name of the server running as pid.
func serverIPCPath(pid int) string {
	return fmt.Sprintf("/operator.server.%d", pid)
}

// newTestApplication creates the server application, and drops it when the
// test ends.
func newTestApplication(t *testing.T) *application {
	t.Helper()
	app, err := newApplication()
	if err != nil {
		t.Fatalf("newApplication() error = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := app.drop(); err != nil {
			t.Errorf("drop() error = %v, want nil", err)
		}
	})
	return app
}

// tryNewApplication creates the server application, drops it when the test
// ends if it was created, and returns the creation error.
func tryNewApplication(t *testing.T) error {
	t.Helper()
	app, err := newApplication()
	if err == nil {
		t.Cleanup(func() {
			if err := app.drop(); err != nil {
				t.Errorf("drop() error = %v, want nil", err)
			}
		})
	}
	return err
}

// newBrokenApplication returns an application whose inbound queue is already
// closed and removed, so that receiving from it fails with EBADF.
func newBrokenApplication(t *testing.T) *application {
	t.Helper()
	app, err := newApplication()
	if err != nil {
		t.Fatalf("newApplication() error = %v, want nil", err)
	}
	if err := app.inbound.Drop(); err != nil {
		t.Fatalf("Drop() error = %v, want nil", err)
	}
	return app
}

// tryConnect opens and closes a sender on the queue ipcPath, and returns the
// open error.
func tryConnect(t *testing.T, ipcPath string) error {
	t.Helper()
	s, err := mqueue.NewSender(ipcPath)
	if err == nil {
		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	}
	return err
}

// send sends frame to the queue ipcPath.
func send(t *testing.T, ipcPath string, frame string) {
	t.Helper()
	s, err := mqueue.NewSender(ipcPath)
	if err != nil {
		t.Fatalf("NewSender(%q) error = %v, want nil", ipcPath, err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	}()
	if err := s.Send([]byte(frame)); err != nil {
		t.Fatalf("Send(%q) error = %v, want nil", frame, err)
	}
}

// cancelledContext returns a context that is already done.
func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// requestedShutdown returns a shutdown that is already requested.
func requestedShutdown() lifecycle.GracefulShutdown {
	requestedChan := make(chan struct{})
	close(requestedChan)
	return lifecycle.NewGracefulShutdown(requestedChan)
}

func TestNewApplicationCreatesQueue(t *testing.T) {
	app := newTestApplication(t)

	want := serverIPCPath(os.Getpid())
	if got := app.inbound.IPCPath(); got != want {
		t.Errorf("newApplication() IPCPath() = %q, want %q", got, want)
	}
	if err := tryConnect(t, want); err != nil {
		t.Errorf("NewSender(%q) error = %v, want nil", want, err)
	}
}

func TestNewApplicationRejectsExistingQueue(t *testing.T) {
	newTestApplication(t)

	if err := tryNewApplication(t); !errors.Is(err, os.ErrExist) {
		t.Errorf("newApplication() with an existing queue error = %v, want %v", err, os.ErrExist)
	}
}

func TestDropWithoutInbound(t *testing.T) {
	var app application

	if err := app.drop(); err != nil {
		t.Errorf("drop() without an inbound error = %v, want nil", err)
	}
}

func TestDropRemovesQueue(t *testing.T) {
	app, err := newApplication()
	if err != nil {
		t.Fatalf("newApplication() error = %v, want nil", err)
	}
	ipcPath := app.inbound.IPCPath()

	if err := app.drop(); err != nil {
		t.Fatalf("drop() error = %v, want nil", err)
	}

	if err := tryConnect(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after drop() error = %v, want %v", ipcPath, err, os.ErrNotExist)
	}
}

func TestListenStopsWhenReady(t *testing.T) {
	app := newTestApplication(t)
	c := startCall(t, context.Background(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, requestedShutdown())
	})

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("listen() after a shutdown request code = %d, want %d", res.Code(), exitcode.Success)
	}
	if res.Err() != nil {
		t.Errorf("listen() after a shutdown request error = %v, want nil", res.Err())
	}
}

func TestListenStopsWhenCancelled(t *testing.T) {
	app := newTestApplication(t)
	c := startCall(t, context.Background(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})

	c.cancel()

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("listen() with a cancelled context code = %d, want %d", res.Code(), exitcode.Success)
	}
	if !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() with a cancelled context error = %v, want %v", res.Err(), context.Canceled)
	}
}

func TestListenSkipsInvalidFrame(t *testing.T) {
	app := newTestApplication(t)
	stderr := captureStderr(t)
	requestedChan := make(chan struct{})
	c := startCall(t, context.Background(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.NewGracefulShutdown(requestedChan))
	})

	send(t, app.inbound.IPCPath(), invalidFrame)
	stderr.waitFor(t, ipc.ErrInvalidFrame.Error())
	close(requestedChan)

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("listen() after an invalid frame then a shutdown request code = %d, want %d", res.Code(), exitcode.Success)
	}
	if res.Err() != nil {
		t.Errorf("listen() after an invalid frame then a shutdown request error = %v, want nil", res.Err())
	}
}

func TestListenSkipsUnhandledOpcode(t *testing.T) {
	app := newTestApplication(t)
	stderr := captureStderr(t)
	requestedChan := make(chan struct{})
	c := startCall(t, context.Background(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.NewGracefulShutdown(requestedChan))
	})

	send(t, app.inbound.IPCPath(), validFrame)
	stderr.waitFor(t, errUnhandledOpcode.Error())
	close(requestedChan)

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("listen() after an unhandled opcode then a shutdown request code = %d, want %d", res.Code(), exitcode.Success)
	}
	if res.Err() != nil {
		t.Errorf("listen() after an unhandled opcode then a shutdown request error = %v, want nil", res.Err())
	}
}

func TestListenReportsReceiveFailure(t *testing.T) {
	app := newBrokenApplication(t)
	c := startCall(t, context.Background(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})

	res := c.wait(t)
	if res.Code() != exitcode.IPC {
		t.Errorf("listen() on a closed queue code = %d, want %d", res.Code(), exitcode.IPC)
	}
	if !errors.Is(res.Err(), syscall.EBADF) {
		t.Errorf("listen() on a closed queue error = %v, want %v", res.Err(), syscall.EBADF)
	}
}

func TestRunReturnsWhenAlreadySignalled(t *testing.T) {
	app := newTestApplication(t)
	c := startCall(t, cancelledContext(), "run()", app.run)

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("run() with a done context code = %d, want %d", res.Code(), exitcode.Success)
	}
	if !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("run() with a done context error = %v, want %v", res.Err(), context.Canceled)
	}
}

func TestRunStopsAfterSignal(t *testing.T) {
	app := newTestApplication(t)
	stderr := captureStderr(t)
	c := startCall(t, context.Background(), "run()", app.run)

	// The frame shows that listen is running before the signal arrives.
	send(t, app.inbound.IPCPath(), validFrame)
	stderr.waitFor(t, errUnhandledOpcode.Error())
	// Cancelling the context of run plays the role of SIGINT.
	c.cancel()

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("run() after a signal code = %d, want %d", res.Code(), exitcode.Success)
	}
	if res.Err() != nil {
		t.Errorf("run() after a signal error = %v, want nil", res.Err())
	}
}

func TestRunReportsReceiveFailure(t *testing.T) {
	app := newBrokenApplication(t)
	c := startCall(t, context.Background(), "run()", app.run)

	res := c.wait(t)
	if res.Code() != exitcode.IPC {
		t.Errorf("run() on a closed queue code = %d, want %d", res.Code(), exitcode.IPC)
	}
	if !errors.Is(res.Err(), syscall.EBADF) {
		t.Errorf("run() on a closed queue error = %v, want %v", res.Err(), syscall.EBADF)
	}
}
