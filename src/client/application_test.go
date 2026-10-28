package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"tree_nity/src/client/commands"
	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
)

const (
	// testTimeout is the longest a test waits for an event before it fails.
	testTimeout = 5 * time.Second
	// shortGracePeriod is a grace period short enough to wait for in a test.
	shortGracePeriod = 10 * time.Millisecond
	// shortCancelTimeout is a cancel timeout short enough to wait for in a test.
	shortCancelTimeout = 10 * time.Millisecond
	// shortRecvTimeout is a receive timeout short enough to wait for in a test.
	shortRecvTimeout = 10 * time.Millisecond
)

// errUnexpectedEncode reports an encoding that the test did not expect.
var errUnexpectedEncode = errors.New("unexpected encode")

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

// mockCommand is a command that runs the function given by the test.
type mockCommand func(ctx context.Context, env commands.Env) (int, error)

// Execute calls the function given by the test.
func (m mockCommand) Execute(ctx context.Context, env commands.Env) (int, error) {
	return m(ctx, env)
}

// testServerIPCPath returns the name of a queue standing for the server, for
// the test process pid.
func testServerIPCPath(pid int) string {
	return fmt.Sprintf("/test.client.server.%d", pid)
}

// newTestQueue creates the queue ipcPath, and drops it when the test ends.
func newTestQueue(t *testing.T, ipcPath string) {
	t.Helper()
	r, err := mqueue.NewReceiver(ipcPath)
	if err != nil {
		t.Fatalf("NewReceiver(%q) error = %v, want nil", ipcPath, err)
	}
	t.Cleanup(func() {
		if err := r.Drop(); err != nil {
			t.Errorf("Drop() error = %v, want nil", err)
		}
	})
}

// testConfig returns an appConfig that connects to serverIPCPath.
func testConfig(serverIPCPath string) appConfig {
	return appConfig{
		serverIPCPath: serverIPCPath,
		encoder: func(protocol.Data) ([]byte, error) {
			return nil, errUnexpectedEncode
		},
		decoder:     protocol.NewDataFromFrame,
		gracePeriod: testTimeout,
	}
}

// newTestApplication creates the client application, and drops it when the
// test ends.
func newTestApplication(t *testing.T, config appConfig) *application {
	t.Helper()
	app, err := newApplication(config)
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

// tryNewApplication creates the client application, drops it when the test
// ends if it was created, and returns the creation error.
func tryNewApplication(t *testing.T, config appConfig) error {
	t.Helper()
	app, err := newApplication(config)
	if err == nil {
		t.Cleanup(func() {
			if err := app.drop(); err != nil {
				t.Errorf("drop() error = %v, want nil", err)
			}
		})
	}
	return err
}

// newFailedShutdownApplication creates the client application as a failed
// graceful shutdown leaves it, and closes its queues when the test ends.
func newFailedShutdownApplication(t *testing.T, config appConfig) *application {
	t.Helper()
	app, err := newApplication(config)
	if err != nil {
		t.Fatalf("newApplication() error = %v, want nil", err)
	}
	app.gracefulShutdownFailed = true
	t.Cleanup(func() {
		if err := app.inbound.Drop(); err != nil {
			t.Errorf("Drop() error = %v, want nil", err)
		}
		if err := app.outbound.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	})
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

// cancelledContext returns a context that is already done.
func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestNewApplicationOpensQueues(t *testing.T) {
	serverIPCPath := testServerIPCPath(os.Getpid())
	newTestQueue(t, serverIPCPath)

	app := newTestApplication(t, testConfig(serverIPCPath))

	if got := app.outbound.IPCPath(); got != serverIPCPath {
		t.Errorf("newApplication() outbound IPCPath() = %q, want %q", got, serverIPCPath)
	}
	ipcPath := clientIPCPath(os.Getpid())
	if got := app.inbound.IPCPath(); got != ipcPath {
		t.Errorf("newApplication() inbound IPCPath() = %q, want %q", got, ipcPath)
	}
	if err := tryConnect(t, ipcPath); err != nil {
		t.Errorf("NewSender(%q) error = %v, want nil", ipcPath, err)
	}
}

func TestNewApplicationRejectsMissingServer(t *testing.T) {
	serverIPCPath := testServerIPCPath(os.Getpid())

	if err := tryNewApplication(t, testConfig(serverIPCPath)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("newApplication() without a server queue error = %v, want %v", err, os.ErrNotExist)
	}
	ipcPath := clientIPCPath(os.Getpid())
	if err := tryConnect(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after newApplication() error = %v, want %v", ipcPath, err, os.ErrNotExist)
	}
}

func TestNewApplicationRejectsInvalidIPCIdentifier(t *testing.T) {
	if err := tryNewApplication(t, testConfig("")); !errors.Is(err, mqueue.ErrInvalidName) {
		t.Errorf("newApplication() with IPC identifier %q error = %v, want %v", "", err, mqueue.ErrInvalidName)
	}
	ipcPath := clientIPCPath(os.Getpid())
	if err := tryConnect(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after newApplication() error = %v, want %v", ipcPath, err, os.ErrNotExist)
	}
}

func TestNewApplicationRejectsExistingClientQueue(t *testing.T) {
	serverIPCPath := testServerIPCPath(os.Getpid())
	newTestQueue(t, serverIPCPath)
	ipcPath := clientIPCPath(os.Getpid())
	newTestQueue(t, ipcPath)

	if err := tryNewApplication(t, testConfig(serverIPCPath)); !errors.Is(err, os.ErrExist) {
		t.Errorf("newApplication() with an existing client queue error = %v, want %v", err, os.ErrExist)
	}
	// newApplication must not remove a queue it did not create.
	if err := tryConnect(t, ipcPath); err != nil {
		t.Errorf("NewSender(%q) after newApplication() error = %v, want nil", ipcPath, err)
	}
}

func TestDropWithoutQueues(t *testing.T) {
	var app application

	if err := app.drop(); err != nil {
		t.Errorf("drop() without queues error = %v, want nil", err)
	}
}

func TestDropRemovesClientQueue(t *testing.T) {
	serverIPCPath := testServerIPCPath(os.Getpid())
	newTestQueue(t, serverIPCPath)
	app, err := newApplication(testConfig(serverIPCPath))
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

func TestDropKeepsServerQueue(t *testing.T) {
	serverIPCPath := testServerIPCPath(os.Getpid())
	newTestQueue(t, serverIPCPath)
	app, err := newApplication(testConfig(serverIPCPath))
	if err != nil {
		t.Fatalf("newApplication() error = %v, want nil", err)
	}

	if err := app.drop(); err != nil {
		t.Fatalf("drop() error = %v, want nil", err)
	}

	if err := tryConnect(t, serverIPCPath); err != nil {
		t.Errorf("NewSender(%q) after drop() error = %v, want nil", serverIPCPath, err)
	}
}

func TestRunReturnsCommandResult(t *testing.T) {
	errCommand := errors.New("command failed")
	app := &application{}
	command := mockCommand(func(ctx context.Context, env commands.Env) (int, error) {
		return exitcode.Topic, errCommand
	})

	code, err := app.run(context.Background(), command)

	if code != exitcode.Topic {
		t.Errorf("run() code = %d, want %d", code, exitcode.Topic)
	}
	if !errors.Is(err, errCommand) {
		t.Errorf("run() error = %v, want %v", err, errCommand)
	}
}

func TestRunPassesApplicationQueuesToCommand(t *testing.T) {
	app := &application{
		inbound:  &ipc.Inbound{},
		outbound: &ipc.Outbound{},
	}
	var got commands.Env
	command := mockCommand(func(ctx context.Context, env commands.Env) (int, error) {
		got = env
		return exitcode.Success, nil
	})

	if _, err := app.run(context.Background(), command); err != nil {
		t.Errorf("run() error = %v, want nil", err)
	}

	if got.Inbound != app.inbound {
		t.Errorf("run() env.Inbound = %p, want %p", got.Inbound, app.inbound)
	}
	if got.Outbound != app.outbound {
		t.Errorf("run() env.Outbound = %p, want %p", got.Outbound, app.outbound)
	}
}

func TestRunDoesNotRequestShutdownWithoutSignal(t *testing.T) {
	app := &application{}
	var requested bool
	command := mockCommand(func(ctx context.Context, env commands.Env) (int, error) {
		requested = env.Shutdown.Requested()
		return exitcode.Success, nil
	})

	if _, err := app.run(context.Background(), command); err != nil {
		t.Errorf("run() error = %v, want nil", err)
	}

	if requested {
		t.Errorf("Requested() without a signal = %v, want %v", requested, false)
	}
}

func TestRunRequestsShutdownAfterSignal(t *testing.T) {
	app := &application{gracePeriod: testTimeout}
	var requested bool
	command := mockCommand(func(ctx context.Context, env commands.Env) (int, error) {
		requested = env.Shutdown.Requested()
		return exitcode.Success, nil
	})

	if _, err := app.run(cancelledContext(), command); err != nil {
		t.Errorf("run() after a signal error = %v, want nil", err)
	}

	if !requested {
		t.Errorf("Requested() after a signal = %v, want %v", requested, true)
	}
}

func TestRunLetsCommandFinishAfterSignal(t *testing.T) {
	app := &application{gracePeriod: testTimeout}
	release := make(chan struct{})
	command := mockCommand(func(ctx context.Context, env commands.Env) (int, error) {
		<-release
		return exitcode.Success, ctx.Err()
	})
	c := startCall(t, cancelledContext(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, command)
	})

	close(release)

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("run() after a signal then the end of the command code = %d, want %d", res.Code(), exitcode.Success)
	}
	if res.Err() != nil {
		t.Errorf("run() after a signal then the end of the command error = %v, want nil", res.Err())
	}
}

func TestRunCancelsCommandAfterGracePeriod(t *testing.T) {
	app := &application{gracePeriod: shortGracePeriod, cancelTimeout: testTimeout}
	command := mockCommand(func(ctx context.Context, env commands.Env) (int, error) {
		<-ctx.Done()
		return exitcode.Success, ctx.Err()
	})
	c := startCall(t, cancelledContext(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, command)
	})

	res := c.wait(t)
	if !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("run() after a signal and the grace period error = %v, want %v", res.Err(), context.Canceled)
	}
}

func TestRunStopsWaitingForCommandIgnoringCancellation(t *testing.T) {
	app := &application{gracePeriod: shortGracePeriod, cancelTimeout: shortCancelTimeout}
	release := make(chan struct{})
	command := mockCommand(func(ctx context.Context, env commands.Env) (int, error) {
		<-release
		return exitcode.Success, nil
	})
	c := startCall(t, cancelledContext(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, command)
	})
	t.Cleanup(func() {
		close(release)
	})

	res := c.wait(t)
	if res.Code() != exitcode.General {
		t.Errorf("run() with a command ignoring the cancellation code = %d, want %d", res.Code(), exitcode.General)
	}
	if !errors.Is(res.Err(), errCommandNotStopped) {
		t.Errorf("run() with a command ignoring the cancellation error = %v, want %v", res.Err(), errCommandNotStopped)
	}
}

func TestRunRecordsFailedGracefulShutdown(t *testing.T) {
	app := &application{gracePeriod: shortGracePeriod, cancelTimeout: shortCancelTimeout}
	release := make(chan struct{})
	command := mockCommand(func(ctx context.Context, env commands.Env) (int, error) {
		<-release
		return exitcode.Success, nil
	})
	c := startCall(t, cancelledContext(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, command)
	})
	t.Cleanup(func() {
		close(release)
	})

	c.wait(t)
	if !app.gracefulShutdownFailed {
		t.Errorf("gracefulShutdownFailed after a command ignoring the cancellation = %v, want %v", app.gracefulShutdownFailed, true)
	}
}

func TestRunKeepsGracefulShutdownWhenCommandStops(t *testing.T) {
	app := &application{gracePeriod: shortGracePeriod, cancelTimeout: testTimeout}
	command := mockCommand(func(ctx context.Context, env commands.Env) (int, error) {
		<-ctx.Done()
		return exitcode.Success, ctx.Err()
	})
	c := startCall(t, cancelledContext(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, command)
	})

	c.wait(t)
	if app.gracefulShutdownFailed {
		t.Errorf("gracefulShutdownFailed after a command stopping once cancelled = %v, want %v", app.gracefulShutdownFailed, false)
	}
}

func TestDropAfterFailedGracefulShutdownRemovesClientQueue(t *testing.T) {
	serverIPCPath := testServerIPCPath(os.Getpid())
	newTestQueue(t, serverIPCPath)
	app := newFailedShutdownApplication(t, testConfig(serverIPCPath))

	if err := app.drop(); err != nil {
		t.Fatalf("drop() after a failed graceful shutdown error = %v, want nil", err)
	}

	ipcPath := app.inbound.IPCPath()
	if err := tryConnect(t, ipcPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("NewSender(%q) after drop() error = %v, want %v", ipcPath, err, os.ErrNotExist)
	}
}

func TestDropAfterFailedGracefulShutdownKeepsQueuesOpen(t *testing.T) {
	serverIPCPath := testServerIPCPath(os.Getpid())
	newTestQueue(t, serverIPCPath)
	config := testConfig(serverIPCPath)
	config.encoder = func(protocol.Data) ([]byte, error) {
		return []byte("frame"), nil
	}
	app := newFailedShutdownApplication(t, config)

	if err := app.drop(); err != nil {
		t.Fatalf("drop() after a failed graceful shutdown error = %v, want nil", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), shortRecvTimeout)
	defer cancel()
	if _, err := app.inbound.Recv(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Recv() after drop() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if err := app.outbound.Send(context.Background(), protocol.Data{}); err != nil {
		t.Errorf("Send() after drop() error = %v, want nil", err)
	}
}
