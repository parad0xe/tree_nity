package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
	"tree_nity/src/server/response"
	"tree_nity/src/server/session/consumer"
	"tree_nity/src/server/session/topic"
)

const (
	// testTimeout is the longest a test waits for an event before it fails.
	testTimeout = 5 * time.Second
	// validFrame decodes to a heartbeat for an unknown consumer.
	validFrame = `{"opcode":8,"emitter":0,"metadata":{"clientIPC":"/client-1"},"payload":{"clientId":"client-1"}}`
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

// TestNewApplicationInitializesTopicManager checks that topic state is ready before listening.
func TestNewApplicationInitializesTopicManager(t *testing.T) {
	app := newTestApplication(t)
	if app.topicManager == nil {
		t.Error("newApplication() topic manager = nil, want an initialized manager")
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

// TestListenRejectsUnknownSubscriber checks rejection delivery before graceful shutdown.
func TestListenRejectsUnknownSubscriber(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	requestedChan := make(chan struct{})
	c := startCall(t, context.Background(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.NewGracefulShutdown(requestedChan))
	})

	data, err := protocol.NewData(protocol.ConsumerInfo, protocol.NewClientMetadata(inbound.IPCPath()), protocol.NewConsumerInfoPayload("consumer-1"))
	if err != nil {
		t.Fatalf("NewData(ConsumerInfo, %q) error = %v, want nil", inbound.IPCPath(), err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(ConsumerInfo) error = %v, want nil", err)
	}
	send(t, app.inbound.IPCPath(), string(frame))
	data = receiveServerReply(t, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok {
		t.Errorf("ConsumerInfo rejection payload = %T, want *protocol.ErrorReplyPayload", data.Payload())
	} else if reply.Kind() != protocol.ClientNotFound || reply.Reason() == "" {
		t.Errorf("ConsumerInfo rejection = %v, %q, want %v and a nonempty reason", reply.Kind(), reply.Reason(), protocol.ClientNotFound)
	}
	close(requestedChan)

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("listen() after an unknown subscriber then a shutdown request code = %d, want %d", res.Code(), exitcode.Success)
	}
	if res.Err() != nil {
		t.Errorf("listen() after an unknown subscriber then a shutdown request error = %v, want nil", res.Err())
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

func TestRunReturnsWhenAlreadyCancelled(t *testing.T) {
	app := newTestApplication(t)
	c := startCall(t, cancelledContext(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, lifecycle.GracefulShutdown{})
	})

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("run() with a done context code = %d, want %d", res.Code(), exitcode.Success)
	}
	if !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("run() with a done context error = %v, want %v", res.Err(), context.Canceled)
	}
}

// TestRunReturnsWhenShutdownAlreadyRequested checks shutdown independently of cancellation.
func TestRunReturnsWhenShutdownAlreadyRequested(t *testing.T) {
	app := newTestApplication(t)
	c := startCall(t, t.Context(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, requestedShutdown())
	})

	res := c.wait(t)
	if res.Code() != exitcode.Success || res.Err() != nil {
		t.Errorf("run() with shutdown already requested = %d, %v, want %d and nil", res.Code(), res.Err(), exitcode.Success)
	}
}

func TestRunStopsAfterSignal(t *testing.T) {
	app := newTestApplication(t)
	stderr := captureStderr(t)
	requestedChan := make(chan struct{})
	c := startCall(t, t.Context(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, lifecycle.NewGracefulShutdown(requestedChan))
	})

	// The frame shows that listen is running before the signal arrives.
	send(t, app.inbound.IPCPath(), validFrame)
	stderr.waitFor(t, consumer.ErrNotFound.Error())
	// Requesting shutdown plays the role of SIGINT without canceling run's context.
	close(requestedChan)

	res := c.wait(t)
	if res.Code() != exitcode.Success {
		t.Errorf("run() after a signal code = %d, want %d", res.Code(), exitcode.Success)
	}
	if res.Err() != nil {
		t.Errorf("run() after a signal error = %v, want nil", res.Err())
	}
}

// TestRunStopsWhenCancelled checks interruption without a graceful shutdown request.
func TestRunStopsWhenCancelled(t *testing.T) {
	app := newTestApplication(t)
	stderr := captureStderr(t)
	c := startCall(t, t.Context(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, lifecycle.GracefulShutdown{})
	})

	send(t, app.inbound.IPCPath(), validFrame)
	stderr.waitFor(t, consumer.ErrNotFound.Error())
	c.cancel()

	res := c.wait(t)
	if res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("run() with a canceled context and no shutdown request = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestRunFinishesReceivedRequestAfterShutdown checks that a signal leaves IPC active.
func TestRunFinishesReceivedRequestAfterShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	received := make(chan struct{})
	resume := make(chan struct{})
	ipcPath := serverIPCPath(os.Getpid())
	inbound, err := ipc.NewInbound(ipcPath, func(frame []byte) (protocol.Data, error) {
		data, err := protocol.Decode(frame)
		if err != nil {
			return data, err
		}
		close(received)
		select {
		case <-resume:
			return data, nil
		case <-ctx.Done():
			return protocol.Data{}, ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("NewInbound(%q) during setup error = %v, want nil", ipcPath, err)
	}
	app := &application{inbound: inbound, topicManager: topic.NewManager()}
	t.Cleanup(func() {
		if err := app.drop(); err != nil {
			t.Errorf("drop() after a request and shutdown error = %v, want nil", err)
		}
	})
	client := newServerClientInbox(t)
	requestedChan := make(chan struct{})
	c := startCall(t, ctx, "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, lifecycle.NewGracefulShutdown(requestedChan))
	})

	sendCreateRequest(t, app, client.IPCPath(), "events")
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatalf("run() did not receive CreateTopic(events) before shutdown: error = %v, want a received request", ctx.Err())
	}
	close(requestedChan)
	close(resume)

	data := receiveServerReply(t, client, protocol.CreateTopicReply)
	if _, ok := data.Payload().(*protocol.CreateTopicReplyPayload); !ok {
		t.Errorf("CreateTopic(events) after shutdown reply payload = %T, want *protocol.CreateTopicReplyPayload", data.Payload())
	}
	res := c.wait(t)
	if res.Code() != exitcode.Success || res.Err() != nil {
		t.Errorf("run() after receiving CreateTopic(events) then shutdown = %d, %v, want %d and nil", res.Code(), res.Err(), exitcode.Success)
	}
}

func TestRunReportsReceiveFailure(t *testing.T) {
	app := newBrokenApplication(t)
	c := startCall(t, context.Background(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, lifecycle.GracefulShutdown{})
	})

	res := c.wait(t)
	if res.Code() != exitcode.IPC {
		t.Errorf("run() on a closed queue code = %d, want %d", res.Code(), exitcode.IPC)
	}
	if !errors.Is(res.Err(), syscall.EBADF) {
		t.Errorf("run() on a closed queue error = %v, want %v", res.Err(), syscall.EBADF)
	}
}

// newServerClientInbox creates a reply queue and removes it after the test.
func newServerClientInbox(t *testing.T) *ipc.Inbound {
	t.Helper()
	ipcPath := fmt.Sprintf("/server-test.%d.%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "."))
	inbound, err := ipc.NewInbound(ipcPath, protocol.Decode)
	if err != nil {
		t.Fatalf("NewInbound(%q) error = %v, want nil", ipcPath, err)
	}
	t.Cleanup(func() {
		if err := inbound.Drop(); err != nil {
			t.Errorf("Drop(%q) error = %v, want nil", ipcPath, err)
		}
	})
	return inbound
}

// sendCreateRequest sends a topic creation request through the server queue.
func sendCreateRequest(t *testing.T, app *application, clientIPC, topicID string) {
	t.Helper()
	data, err := protocol.NewData(protocol.CreateTopic, protocol.NewClientMetadata(clientIPC), protocol.NewCreateTopicPayload(topicID))
	if err != nil {
		t.Fatalf("NewData(CreateTopic, %q, %q) error = %v, want nil", clientIPC, topicID, err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(CreateTopic, %q, %q) error = %v, want nil", clientIPC, topicID, err)
	}
	send(t, app.inbound.IPCPath(), string(frame))
}

// sendCheckRequest sends a topic check through the server queue.
func sendCheckRequest(t *testing.T, app *application, clientIPC, topicID string) {
	t.Helper()
	data, err := protocol.NewData(protocol.CheckTopic, protocol.NewClientMetadata(clientIPC), protocol.NewCheckTopicPayload(topicID))
	if err != nil {
		t.Fatalf("NewData(CheckTopic, %q, %q) error = %v, want nil", clientIPC, topicID, err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(CheckTopic, %q, %q) error = %v, want nil", clientIPC, topicID, err)
	}
	send(t, app.inbound.IPCPath(), string(frame))
}

// receiveServerReply checks the opcode and emitter of a response from the running server.
func receiveServerReply(t *testing.T, inbound *ipc.Inbound, opcode protocol.Opcode) protocol.Data {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	data, err := inbound.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv(%q) error = %v, want nil", inbound.IPCPath(), err)
	}
	if data.Emitter() != protocol.Server || data.Opcode() != opcode {
		t.Errorf("Recv(%q) emitter = %v, opcode = %v, want %v and %v", inbound.IPCPath(), data.Emitter(), data.Opcode(), protocol.Server, opcode)
	}
	return data
}

// TestDispatchListsTopics checks the names returned for empty and populated registries.
func TestDispatchListsTopics(t *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "empty"},
		{name: "single topic", count: 1},
		{name: "many topics", count: 128},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			app := &application{topicManager: topic.NewManager()}
			inbound := newServerClientInbox(t)
			metadata := protocol.NewClientMetadata(inbound.IPCPath())
			want := make([]string, tt.count)
			for i := range want {
				want[i] = fmt.Sprintf("topic-%03d", i)
				data, err := protocol.NewData(protocol.CreateTopic, metadata, protocol.NewCreateTopicPayload(want[i]))
				if err != nil {
					t.Fatalf("NewData(CreateTopic, %q) during setup error = %v, want nil", want[i], err)
				}
				if code, err := app.dispatch(ctx, lifecycle.GracefulShutdown{}, data); code != exitcode.Success || err != nil {
					t.Fatalf("dispatch(CreateTopic, %q) during setup = %d, %v, want %d and nil", want[i], code, err, exitcode.Success)
				}
				receiveServerReply(t, inbound, protocol.CreateTopicReply)
			}
			data, err := protocol.NewData(protocol.ListTopic, metadata, protocol.NewListTopicPayload())
			if err != nil {
				t.Fatalf("NewData(ListTopic) during setup error = %v, want nil", err)
			}
			if code, err := app.dispatch(ctx, lifecycle.GracefulShutdown{}, data); code != exitcode.Success || err != nil {
				t.Errorf("dispatch(ListTopic) with %d topics = %d, %v, want %d and nil", tt.count, code, err, exitcode.Success)
				return
			}
			data = receiveServerReply(t, inbound, protocol.ListTopicReply)
			reply, ok := data.Payload().(*protocol.ListTopicReplyPayload)
			if !ok {
				t.Errorf("dispatch(ListTopic) with %d topics reply payload = %T, want *protocol.ListTopicReplyPayload", tt.count, data.Payload())
				return
			}
			got := reply.Topics()
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Errorf("dispatch(ListTopic) with topics %q = %q, want the same names in any order", want, got)
			}
		})
	}
}

// TestDispatchRejectsUnknownSubscriber checks the consumer lookup rejection.
func TestDispatchRejectsUnknownSubscriber(t *testing.T) {
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
	}{
		{name: "consumer info", opcode: protocol.ConsumerInfo, payload: protocol.NewConsumerInfoPayload("consumer-1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager()}
			inbound := newServerClientInbox(t)
			data, err := protocol.NewData(tt.opcode, protocol.NewClientMetadata(inbound.IPCPath()), tt.payload)
			if err != nil {
				t.Fatalf("NewData(%v, %q) during setup error = %v, want nil", tt.opcode, inbound.IPCPath(), err)
			}
			if code, err := app.dispatch(t.Context(), lifecycle.GracefulShutdown{}, data); code != exitcode.Success || err != nil {
				t.Errorf("dispatch(%v, %q, %T) = %d, %v, want %d and nil after rejection", tt.opcode, inbound.IPCPath(), tt.payload, code, err, exitcode.Success)
				return
			}
			data = receiveServerReply(t, inbound, protocol.ErrorReply)
			reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
			if !ok {
				t.Errorf("dispatch(%v) rejection payload = %T, want *protocol.ErrorReplyPayload", tt.opcode, data.Payload())
			} else if reply.Kind() != protocol.ClientNotFound || reply.Reason() == "" {
				t.Errorf("dispatch(%v) rejection = %v, %q, want %v and a nonempty reason", tt.opcode, reply.Kind(), reply.Reason(), protocol.ClientNotFound)
			}
		})
	}
}

// TestDispatchDoesNotReplyToNotifications checks one-way requests without timing assumptions.
func TestDispatchDoesNotReplyToNotifications(t *testing.T) {
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
		wantErr error
	}{
		{name: "acknowledgement", opcode: protocol.Acknowledgement, payload: protocol.NewAcknowledgementPayload("client-1", 1), wantErr: consumer.ErrNotFound},
		{name: "heartbeat", opcode: protocol.ConsumerHeartbeat, payload: protocol.NewConsumerHeartbeatPayload("client-1"), wantErr: consumer.ErrNotFound},
		{name: "disconnect", opcode: protocol.ConsumerDisconnect, payload: protocol.NewConsumerDisconnectPayload("client-1"), wantErr: consumer.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager()}
			inbound := newServerClientInbox(t)
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			data, err := protocol.NewData(tt.opcode, &metadata, tt.payload)
			if err != nil {
				t.Fatalf("NewData(%v, %q) during setup error = %v, want nil", tt.opcode, inbound.IPCPath(), err)
			}
			if code, err := app.dispatch(t.Context(), lifecycle.GracefulShutdown{}, data); code != exitcode.General || !errors.Is(err, tt.wantErr) {
				t.Errorf("dispatch(%v, %q) = %d, %v, want %d and %v", tt.opcode, inbound.IPCPath(), code, err, exitcode.General, tt.wantErr)
			}
			// A marker sent after dispatch must be the first frame in the client queue.
			if err := response.SendToClient(t.Context(), metadata, protocol.ServerShutdown, protocol.NewServerShutdownPayload()); err != nil {
				t.Fatalf("SendToClient(ServerShutdown) after dispatch(%v) error = %v, want nil", tt.opcode, err)
			}
			receiveServerReply(t, inbound, protocol.ServerShutdown)
		})
	}
}

// TestDispatchConsumerDisconnectReleasesName checks one-way dispatch during normal operation and shutdown.
func TestDispatchConsumerDisconnectReleasesName(t *testing.T) {
	tests := []struct {
		name     string
		shutdown bool
	}{
		{name: "normal operation"},
		{name: "shutdown requested", shutdown: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager()}
			inbound := newServerClientInbox(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			metadata := *protocol.NewClientMetadata(inbound.IPCPath())
			dispatch := func(shutdown lifecycle.GracefulShutdown, opcode protocol.Opcode, payload protocol.Payload) {
				t.Helper()
				data, err := protocol.NewData(opcode, &metadata, payload)
				if err != nil {
					t.Fatalf("NewData(%v) error = %v, want nil", opcode, err)
				}
				if code, err := app.dispatch(ctx, shutdown, data); code != exitcode.Success || err != nil {
					t.Fatalf("dispatch(%v) during %s = %d, %v, want %d and nil", opcode, tt.name, code, err, exitcode.Success)
				}
			}
			dispatch(lifecycle.GracefulShutdown{}, protocol.CreateTopic, protocol.NewCreateTopicPayload("events"))
			receiveServerReply(t, inbound, protocol.CreateTopicReply)
			subscription := protocol.NewTopicSubscriptionPayload("subscriber", "events", "", nil)
			dispatch(lifecycle.GracefulShutdown{}, protocol.TopicSubscription, subscription)
			receiveServerReply(t, inbound, protocol.TopicSubscriptionReply)
			var shutdown lifecycle.GracefulShutdown
			if tt.shutdown {
				shutdown = requestedShutdown()
			}
			dispatch(shutdown, protocol.ConsumerDisconnect, protocol.NewConsumerDisconnectPayload("subscriber"))
			if err := response.SendToClient(ctx, metadata, protocol.ServerShutdown, protocol.NewServerShutdownPayload()); err != nil {
				t.Fatalf("SendToClient(marker) after disconnect during %s error = %v, want nil", tt.name, err)
			}
			receiveServerReply(t, inbound, protocol.ServerShutdown)
			dispatch(lifecycle.GracefulShutdown{}, protocol.TopicSubscription, subscription)
			receiveServerReply(t, inbound, protocol.TopicSubscriptionReply)
		})
	}
}

// TestDispatchConsumerHeartbeatRoutesActiveConsumer checks the empty heartbeat response.
func TestDispatchConsumerHeartbeatRoutesActiveConsumer(t *testing.T) {
	app := &application{topicManager: topic.NewManager()}
	inbound := newServerClientInbox(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	dispatch := func(opcode protocol.Opcode, payload protocol.Payload) {
		t.Helper()
		data, err := protocol.NewData(opcode, &metadata, payload)
		if err != nil {
			t.Fatalf("NewData(%v) error = %v, want nil", opcode, err)
		}
		if code, err := app.dispatch(ctx, lifecycle.GracefulShutdown{}, data); code != exitcode.Success || err != nil {
			t.Fatalf("dispatch(%v) = %d, %v, want %d and nil", opcode, code, err, exitcode.Success)
		}
	}
	dispatch(protocol.CreateTopic, protocol.NewCreateTopicPayload("events"))
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	dispatch(protocol.TopicSubscription, protocol.NewTopicSubscriptionPayload("subscriber", "events", "", nil))
	receiveServerReply(t, inbound, protocol.TopicSubscriptionReply)
	dispatch(protocol.ConsumerHeartbeat, protocol.NewConsumerHeartbeatPayload("subscriber"))
	receiveServerReply(t, inbound, protocol.ConsumerHeartbeatReply)
}

// TestDispatchRejectsServerFrames checks rejection of server opcodes overlapping client opcodes.
func TestDispatchRejectsServerFrames(t *testing.T) {
	app := &application{topicManager: topic.NewManager()}
	data, err := protocol.NewData(protocol.CreateTopicReply, protocol.NewServerMetadata(), protocol.NewCreateTopicReplyPayload())
	if err != nil {
		t.Fatalf("NewData(CreateTopicReply) during setup error = %v, want nil", err)
	}
	if code, err := app.dispatch(t.Context(), lifecycle.GracefulShutdown{}, data); code != exitcode.General || !errors.Is(err, errUnhandledOpcode) {
		t.Errorf("dispatch(CreateTopicReply) = %d, %v, want %d and %v", code, err, exitcode.General, errUnhandledOpcode)
	}
}

// TestDispatchPropagatesRejectionErrors checks failures opening the rejection reply queue.
func TestDispatchPropagatesRejectionErrors(t *testing.T) {
	tests := []struct {
		name    string
		ipcPath string
		wantErr error
	}{
		{name: "invalid name", ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "missing queue", ipcPath: fmt.Sprintf("/server-test.missing.%d", os.Getpid()), wantErr: os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager()}
			data, err := protocol.NewData(protocol.ConsumerInfo, protocol.NewClientMetadata(tt.ipcPath), protocol.NewConsumerInfoPayload("consumer-1"))
			if err != nil {
				t.Fatalf("NewData(ConsumerInfo, %q) during setup error = %v, want nil", tt.ipcPath, err)
			}
			code, err := app.dispatch(t.Context(), lifecycle.GracefulShutdown{}, data)
			if code != exitcode.General || !errors.Is(err, tt.wantErr) || errors.Is(err, errUnhandledOpcode) {
				t.Errorf("dispatch(ConsumerInfo, %q) = %d, %v, want %d and %v from the handler", tt.ipcPath, code, err, exitcode.General, tt.wantErr)
			}
		})
	}
}

// TestDispatchPropagatesStoppedContexts checks reply cancellation and its IPC exit code.
func TestDispatchPropagatesStoppedContexts(t *testing.T) {
	expired, expireCancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer expireCancel()
	tests := []struct {
		name          string
		ctx           context.Context
		opcode        protocol.Opcode
		payload       protocol.Payload
		wantErr       error
		wantUnhandled bool
	}{
		{name: "canceled creation", ctx: cancelledContext(), opcode: protocol.CreateTopic, payload: protocol.NewCreateTopicPayload("events"), wantErr: context.Canceled},
		{name: "expired creation", ctx: expired, opcode: protocol.CreateTopic, payload: protocol.NewCreateTopicPayload("events"), wantErr: context.DeadlineExceeded},
		{name: "canceled listing", ctx: cancelledContext(), opcode: protocol.ListTopic, payload: protocol.NewListTopicPayload(), wantErr: context.Canceled},
		{name: "expired listing", ctx: expired, opcode: protocol.ListTopic, payload: protocol.NewListTopicPayload(), wantErr: context.DeadlineExceeded},
		{name: "canceled check", ctx: cancelledContext(), opcode: protocol.CheckTopic, payload: protocol.NewCheckTopicPayload("events"), wantErr: context.Canceled},
		{name: "expired check", ctx: expired, opcode: protocol.CheckTopic, payload: protocol.NewCheckTopicPayload("events"), wantErr: context.DeadlineExceeded},
		{name: "canceled production rejection", ctx: cancelledContext(), opcode: protocol.ProduceMessage, payload: protocol.NewProduceMessagePayload("missing", protocol.NewMessage(nil, nil)), wantErr: context.Canceled},
		{name: "expired production rejection", ctx: expired, opcode: protocol.ProduceMessage, payload: protocol.NewProduceMessagePayload("missing", protocol.NewMessage(nil, nil)), wantErr: context.DeadlineExceeded},
		{name: "canceled subscription rejection", ctx: cancelledContext(), opcode: protocol.TopicSubscription, payload: protocol.NewTopicSubscriptionPayload("consumer-1", "missing", "", nil), wantErr: context.Canceled},
		{name: "expired subscription rejection", ctx: expired, opcode: protocol.TopicSubscription, payload: protocol.NewTopicSubscriptionPayload("consumer-1", "missing", "", nil), wantErr: context.DeadlineExceeded},
		{name: "canceled consumer info", ctx: cancelledContext(), opcode: protocol.ConsumerInfo, payload: protocol.NewConsumerInfoPayload("consumer-1"), wantErr: context.Canceled},
		{name: "expired consumer info", ctx: expired, opcode: protocol.ConsumerInfo, payload: protocol.NewConsumerInfoPayload("consumer-1"), wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager()}
			inbound := newServerClientInbox(t)
			data, err := protocol.NewData(tt.opcode, protocol.NewClientMetadata(inbound.IPCPath()), tt.payload)
			if err != nil {
				t.Fatalf("NewData(%v, %q) during setup error = %v, want nil", tt.opcode, inbound.IPCPath(), err)
			}
			code, err := app.dispatch(tt.ctx, lifecycle.GracefulShutdown{}, data)
			if code != exitcode.IPC || !errors.Is(err, tt.wantErr) || !errors.Is(err, response.ErrReply) {
				t.Errorf("dispatch(%v) with %s = %d, %v, want %d, %v and %v", tt.opcode, tt.name, code, err, exitcode.IPC, tt.wantErr, response.ErrReply)
			}
			if got := errors.Is(err, errUnhandledOpcode); got != tt.wantUnhandled {
				t.Errorf("dispatch(%v) with %s matches %v = %t, want %t", tt.opcode, tt.name, errUnhandledOpcode, got, tt.wantUnhandled)
			}
		})
	}
}

// fillServerClientInbox fills a queue with shutdown frames and returns their count.
func fillServerClientInbox(t *testing.T, inbound *ipc.Inbound) int {
	t.Helper()
	data, err := protocol.NewData(protocol.ServerShutdown, protocol.NewServerMetadata(), protocol.NewServerShutdownPayload())
	if err != nil {
		t.Fatalf("NewData(ServerShutdown) during setup error = %v, want nil", err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(ServerShutdown) during setup error = %v, want nil", err)
	}
	sender, err := mqueue.NewSender(inbound.IPCPath())
	if err != nil {
		t.Fatalf("NewSender(%q) during setup error = %v, want nil", inbound.IPCPath(), err)
	}
	t.Cleanup(func() {
		if err := sender.Close(); err != nil {
			t.Errorf("Close(%q) after filling the queue error = %v, want nil", inbound.IPCPath(), err)
		}
	})
	for count := 0; ; count++ {
		err := sender.TimedSend(frame, time.Now())
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return count
		}
		if err != nil {
			t.Fatalf("TimedSend(ServerShutdown) to %q at frame %d error = %v, want nil or %v", inbound.IPCPath(), count, err, os.ErrDeadlineExceeded)
		}
	}
}

// TestDispatchTimesOutOnFullClientQueue checks bounded sends with a live listener context.
func TestDispatchTimesOutOnFullClientQueue(t *testing.T) {
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
	}{
		{name: "creation", opcode: protocol.CreateTopic, payload: protocol.NewCreateTopicPayload("events")},
		{name: "listing", opcode: protocol.ListTopic, payload: protocol.NewListTopicPayload()},
		{name: "check", opcode: protocol.CheckTopic, payload: protocol.NewCheckTopicPayload("events")},
		{name: "production rejection", opcode: protocol.ProduceMessage, payload: protocol.NewProduceMessagePayload("missing", protocol.NewMessage(nil, nil))},
		{name: "subscription rejection", opcode: protocol.TopicSubscription, payload: protocol.NewTopicSubscriptionPayload("consumer-1", "missing", "", nil)},
		{name: "consumer info rejection", opcode: protocol.ConsumerInfo, payload: protocol.NewConsumerInfoPayload("consumer-1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager()}
			inbound := newServerClientInbox(t)
			fillServerClientInbox(t, inbound)
			data, err := protocol.NewData(tt.opcode, protocol.NewClientMetadata(inbound.IPCPath()), tt.payload)
			if err != nil {
				t.Fatalf("NewData(%v, %q) during setup error = %v, want nil", tt.opcode, inbound.IPCPath(), err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			if code, err := app.dispatch(ctx, lifecycle.GracefulShutdown{}, data); code != exitcode.IPC || !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("dispatch(%v, %q) with a full client queue = %d, %v, want %d and %v", tt.opcode, inbound.IPCPath(), code, err, exitcode.IPC, context.DeadlineExceeded)
			}
			if err := ctx.Err(); err != nil {
				t.Errorf("listener context after dispatch(%v) to a full queue error = %v, want nil", tt.opcode, err)
			}
		})
	}
}

// TestDispatchTimesOutOnFullProductionReplyQueue checks bounded confirmation delivery.
func TestDispatchTimesOutOnFullProductionReplyQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	app := &application{topicManager: topic.NewManager()}
	inbound := newServerClientInbox(t)
	metadata := protocol.NewClientMetadata(inbound.IPCPath())
	if err := app.topicManager.CreateTopic(ctx, ctx, lifecycle.GracefulShutdown{}, *metadata, *protocol.NewCreateTopicPayload("events")); err != nil {
		t.Errorf("CreateTopic(events) during setup error = %v, want nil", err)
		return
	}
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	fillServerClientInbox(t, inbound)
	data, err := protocol.NewData(protocol.ProduceMessage, metadata, protocol.NewProduceMessagePayload("events", protocol.NewMessage(nil, nil)))
	if err != nil {
		t.Errorf("NewData(ProduceMessage, events) during setup error = %v, want nil", err)
		return
	}
	if code, err := app.dispatch(ctx, lifecycle.GracefulShutdown{}, data); code != exitcode.IPC || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, response.ErrReply) {
		t.Errorf("dispatch(ProduceMessage, events) with a full client queue = %d, %v, want %d, %v and %v", code, err, exitcode.IPC, context.DeadlineExceeded, response.ErrReply)
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("listener context after dispatch(ProduceMessage, events) to a full queue error = %v, want nil", err)
	}
}

// TestDispatchReturnsHandlerErrors checks connection failures and their general exit code.
func TestDispatchReturnsHandlerErrors(t *testing.T) {
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
	}{
		{name: "creation", opcode: protocol.CreateTopic, payload: protocol.NewCreateTopicPayload("events")},
		{name: "check", opcode: protocol.CheckTopic, payload: protocol.NewCheckTopicPayload("events")},
		{name: "subscription", opcode: protocol.TopicSubscription, payload: protocol.NewTopicSubscriptionPayload("consumer-1", "missing", "", nil)},
		{name: "production", opcode: protocol.ProduceMessage, payload: protocol.NewProduceMessagePayload("events", protocol.NewMessage(nil, nil))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager()}
			data, err := protocol.NewData(tt.opcode, protocol.NewClientMetadata("/invalid/queue"), tt.payload)
			if err != nil {
				t.Fatalf("NewData(%v, /invalid/queue) during setup error = %v, want nil", tt.opcode, err)
			}
			if code, err := app.dispatch(t.Context(), lifecycle.GracefulShutdown{}, data); code != exitcode.General || !errors.Is(err, mqueue.ErrInvalidName) {
				t.Errorf("dispatch(%v, /invalid/queue) = %d, %v, want %d and %v", tt.opcode, code, err, exitcode.General, mqueue.ErrInvalidName)
			}
		})
	}
}

// TestDispatchPropagatesProductionCancellation checks errors from the topic channel.
func TestDispatchPropagatesProductionCancellation(t *testing.T) {
	expired, expireCancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer expireCancel()
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{name: "canceled", ctx: cancelledContext(), wantErr: context.Canceled},
		{name: "expired", ctx: expired, wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager()}
			inbound := newServerClientInbox(t)
			metadata := protocol.NewClientMetadata(inbound.IPCPath())
			if err := app.topicManager.CreateTopic(t.Context(), t.Context(), lifecycle.GracefulShutdown{}, *metadata, *protocol.NewCreateTopicPayload("events")); err != nil {
				t.Fatalf("CreateTopic(events) during setup error = %v, want nil", err)
			}
			receiveServerReply(t, inbound, protocol.CreateTopicReply)
			data, err := protocol.NewData(protocol.ProduceMessage, metadata, protocol.NewProduceMessagePayload("events", protocol.NewMessage(nil, nil)))
			if err != nil {
				t.Fatalf("NewData(ProduceMessage, events) during setup error = %v, want nil", err)
			}
			code, err := app.dispatch(tt.ctx, lifecycle.GracefulShutdown{}, data)
			if code != exitcode.General || !errors.Is(err, tt.wantErr) || errors.Is(err, response.ErrReply) {
				t.Errorf("dispatch(ProduceMessage, events) with %s context = %d, %v, want %d and %v from the topic", tt.name, code, err, exitcode.General, tt.wantErr)
			}
		})
	}
}

// TestDispatchProducesWithoutManualReceiver checks the live topic receiver.
func TestDispatchProducesWithoutManualReceiver(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	app := &application{topicManager: topic.NewManager()}
	inbound := newServerClientInbox(t)
	metadata := protocol.NewClientMetadata(inbound.IPCPath())
	if err := app.topicManager.CreateTopic(ctx, ctx, lifecycle.GracefulShutdown{}, *metadata, *protocol.NewCreateTopicPayload("events")); err != nil {
		t.Fatalf("CreateTopic(events) during setup error = %v, want nil", err)
	}
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	data, err := protocol.NewData(protocol.ProduceMessage, metadata, protocol.NewProduceMessagePayload("events", protocol.NewMessage(nil, nil)))
	if err != nil {
		t.Fatalf("NewData(ProduceMessage, events) during setup error = %v, want nil", err)
	}
	if code, err := app.dispatch(ctx, lifecycle.GracefulShutdown{}, data); code != exitcode.Success || err != nil {
		t.Errorf("dispatch(ProduceMessage, events) with the topic receiver = %d, %v, want %d and nil", code, err, exitcode.Success)
		return
	}
	data = receiveServerReply(t, inbound, protocol.ProduceMessageReply)
	if _, ok := data.Payload().(*protocol.ProduceMessageReplyPayload); !ok {
		t.Errorf("dispatch(ProduceMessage, events) reply payload = %T, want *protocol.ProduceMessageReplyPayload", data.Payload())
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("listener context after dispatch(ProduceMessage, events) error = %v, want nil", err)
	}
}

// TestDispatchRejectsProductionDuringShutdown checks signal transmission to the handler.
func TestDispatchRejectsProductionDuringShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	app := &application{topicManager: topic.NewManager()}
	inbound := newServerClientInbox(t)
	metadata := protocol.NewClientMetadata(inbound.IPCPath())
	if err := app.topicManager.CreateTopic(ctx, ctx, lifecycle.GracefulShutdown{}, *metadata, *protocol.NewCreateTopicPayload("events")); err != nil {
		t.Errorf("CreateTopic(events) during setup error = %v, want nil", err)
		return
	}
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	requested := make(chan struct{})
	shutdown := lifecycle.NewGracefulShutdown(requested)
	close(requested)
	data, err := protocol.NewData(protocol.ProduceMessage, metadata, protocol.NewProduceMessagePayload("events", protocol.NewMessage(nil, nil)))
	if err != nil {
		t.Errorf("NewData(ProduceMessage, events) error = %v, want nil", err)
		return
	}
	if code, err := app.dispatch(ctx, shutdown, data); code != exitcode.Success || err != nil {
		t.Errorf("dispatch(ProduceMessage, events) during shutdown = %d, %v, want %d and nil after rejection", code, err, exitcode.Success)
		return
	}
	data = receiveServerReply(t, inbound, protocol.ServerShutdown)
	if _, ok := data.Payload().(*protocol.ServerShutdownPayload); !ok {
		t.Errorf("production rejection during shutdown payload = %T, want *protocol.ServerShutdownPayload", data.Payload())
	}
}

// TestListenProducesWithoutBlockingRequests checks production followed by a query over IPC.
func TestListenProducesWithoutBlockingRequests(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	c := startCall(t, t.Context(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})
	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	for i := 0; i < 8; i++ {
		data, err := protocol.NewData(protocol.ProduceMessage, protocol.NewClientMetadata(inbound.IPCPath()),
			protocol.NewProduceMessagePayload("events", protocol.NewMessage([]byte("key"), []byte{byte(i)})))
		if err != nil {
			t.Errorf("NewData(ProduceMessage, sequence %d) error = %v, want nil", i, err)
			return
		}
		frame, err := protocol.Encode(data)
		if err != nil {
			t.Errorf("Encode(ProduceMessage, sequence %d) error = %v, want nil", i, err)
			return
		}
		send(t, app.inbound.IPCPath(), string(frame))
		data = receiveServerReply(t, inbound, protocol.ProduceMessageReply)
		if _, ok := data.Payload().(*protocol.ProduceMessageReplyPayload); !ok {
			t.Errorf("ProduceMessage(events, sequence %d) reply payload = %T, want *protocol.ProduceMessageReplyPayload", i, data.Payload())
		}
	}
	sendCheckRequest(t, app, inbound.IPCPath(), "events")
	receiveServerReply(t, inbound, protocol.CheckTopicReply)
	c.cancel()
	if res := c.wait(t); res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after production, query and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestListenRejectsProductionForMissingTopic checks rejection and continued service over IPC.
func TestListenRejectsProductionForMissingTopic(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	c := startCall(t, t.Context(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})
	data, err := protocol.NewData(protocol.ProduceMessage, protocol.NewClientMetadata(inbound.IPCPath()), protocol.NewProduceMessagePayload("events", protocol.NewMessage(nil, nil)))
	if err != nil {
		t.Fatalf("NewData(ProduceMessage, events) during setup error = %v, want nil", err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(ProduceMessage, events) during setup error = %v, want nil", err)
	}
	send(t, app.inbound.IPCPath(), string(frame))
	data = receiveServerReply(t, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok {
		t.Errorf("ProduceMessage(events) before creation payload = %T, want *protocol.ErrorReplyPayload", data.Payload())
	} else if reply.Kind() != protocol.TopicNotFound || reply.Reason() == "" {
		t.Errorf("ProduceMessage(events) before creation rejection = %v, %q, want %v and a nonempty reason", reply.Kind(), reply.Reason(), protocol.TopicNotFound)
	}
	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	c.cancel()
	if res := c.wait(t); res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after rejecting production, creating a topic and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestDispatchForwardsSubscriptionShutdown checks signal transmission to the handler.
func TestDispatchForwardsSubscriptionShutdown(t *testing.T) {
	app := &application{topicManager: topic.NewManager()}
	inbound := newServerClientInbox(t)
	requested := make(chan struct{})
	shutdown := lifecycle.NewGracefulShutdown(requested)
	close(requested)
	data, err := protocol.NewData(protocol.TopicSubscription, protocol.NewClientMetadata(inbound.IPCPath()),
		protocol.NewTopicSubscriptionPayload("consumer-1", "events", "user/", nil))
	if err != nil {
		t.Errorf("NewData(TopicSubscription, consumer-1, events) error = %v, want nil", err)
		return
	}
	if code, err := app.dispatch(t.Context(), shutdown, data); code != exitcode.Success || err != nil {
		t.Errorf("dispatch(TopicSubscription, consumer-1, events) during shutdown = %d, %v, want %d and nil", code, err, exitcode.Success)
		return
	}
	data = receiveServerReply(t, inbound, protocol.ServerShutdown)
	if _, ok := data.Payload().(*protocol.ServerShutdownPayload); !ok {
		t.Errorf("subscription during shutdown payload = %T, want *protocol.ServerShutdownPayload", data.Payload())
	}
}

// TestListenRepliesWithConsumerInfo checks querying another subscriber over IPC.
func TestListenRepliesWithConsumerInfo(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	subscriber, err := ipc.NewInbound(inbound.IPCPath()+".subscriber", protocol.Decode)
	if err != nil {
		t.Fatalf("NewInbound(subscriber) during setup error = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := subscriber.Drop(); err != nil {
			t.Errorf("Drop(subscriber) error = %v, want nil", err)
		}
	})
	c := startCall(t, t.Context(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})
	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	offset := uint32(42)
	data, err := protocol.NewData(protocol.TopicSubscription, protocol.NewClientMetadata(subscriber.IPCPath()),
		protocol.NewTopicSubscriptionPayload("subscriber-1", "events", "é/猫/", &offset))
	if err != nil {
		t.Fatalf("NewData(TopicSubscription, subscriber-1) during setup error = %v, want nil", err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(TopicSubscription, subscriber-1) during setup error = %v, want nil", err)
	}
	send(t, app.inbound.IPCPath(), string(frame))
	receiveServerReply(t, subscriber, protocol.TopicSubscriptionReply)
	data, err = protocol.NewData(protocol.ConsumerInfo, protocol.NewClientMetadata(inbound.IPCPath()),
		protocol.NewConsumerInfoPayload("subscriber-1"))
	if err != nil {
		t.Fatalf("NewData(ConsumerInfo, subscriber-1) error = %v, want nil", err)
	}
	frame, err = protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(ConsumerInfo, subscriber-1) error = %v, want nil", err)
	}
	send(t, app.inbound.IPCPath(), string(frame))
	data = receiveServerReply(t, inbound, protocol.ConsumerInfoReply)
	reply, ok := data.Payload().(*protocol.ConsumerInfoReplyPayload)
	if !ok {
		t.Errorf("ConsumerInfo(subscriber-1) payload = %T, want *protocol.ConsumerInfoReplyPayload", data.Payload())
	} else if reply.Client() != "subscriber-1" || reply.Topic() != "events" || reply.Offset() != offset || reply.Prefix() != "é/猫/" || reply.IPC() != subscriber.IPCPath() {
		t.Errorf("ConsumerInfo(subscriber-1) payload = %+v, want events, offset %d, prefix é/猫/ and IPC %q", reply, offset, subscriber.IPCPath())
	}
	c.cancel()
	if res := c.wait(t); res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after ConsumerInfo and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestListenCommitsAcknowledgements checks delivery and immediately queryable progress over IPC.
func TestListenCommitsAcknowledgements(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	producer, err := ipc.NewInbound(inbound.IPCPath()+".producer", protocol.Decode)
	if err != nil {
		t.Fatalf("NewInbound(producer) during setup error = %v, want nil", err)
	}
	t.Cleanup(func() {
		if err := producer.Drop(); err != nil {
			t.Errorf("Drop(producer) error = %v, want nil", err)
		}
	})
	c := startCall(t, t.Context(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})
	request := func(ipcPath string, opcode protocol.Opcode, payload protocol.Payload) {
		data, err := protocol.NewData(opcode, protocol.NewClientMetadata(ipcPath), payload)
		if err != nil {
			t.Fatalf("NewData(%v) error = %v, want nil", opcode, err)
		}
		frame, err := protocol.Encode(data)
		if err != nil {
			t.Fatalf("Encode(%v) error = %v, want nil", opcode, err)
		}
		send(t, app.inbound.IPCPath(), string(frame))
	}
	sendCreateRequest(t, app, producer.IPCPath(), "events")
	receiveServerReply(t, producer, protocol.CreateTopicReply)
	for offset := 0; offset < 2; offset++ {
		request(producer.IPCPath(), protocol.ProduceMessage, protocol.NewProduceMessagePayload("events", protocol.NewMessage([]byte("key"), []byte{byte(offset)})))
		receiveServerReply(t, producer, protocol.ProduceMessageReply)
	}
	request(inbound.IPCPath(), protocol.TopicSubscription, protocol.NewTopicSubscriptionPayload("subscriber", "events", "", nil))
	receiveServerReply(t, inbound, protocol.TopicSubscriptionReply)
	for offset := uint32(0); offset < 2; offset++ {
		data := receiveServerReply(t, inbound, protocol.MessageDelivery)
		delivery, ok := data.Payload().(*protocol.MessageDeliveryPayload)
		if !ok {
			t.Fatalf("MessageDelivery(%d) payload = %T, want *protocol.MessageDeliveryPayload", offset, data.Payload())
		}
		if message := delivery.Message(); message.Offset() != offset || string(message.Key()) != "key" || len(message.Value()) != 1 || message.Value()[0] != byte(offset) {
			t.Errorf("MessageDelivery(%d) = offset %d, key %x, value %x, want key and value %x", offset, message.Offset(), message.Key(), message.Value(), []byte{byte(offset)})
		}
		request(inbound.IPCPath(), protocol.Acknowledgement, protocol.NewAcknowledgementPayload("subscriber", offset+1))
		request(producer.IPCPath(), protocol.ConsumerInfo, protocol.NewConsumerInfoPayload("subscriber"))
		data = receiveServerReply(t, producer, protocol.ConsumerInfoReply)
		info, ok := data.Payload().(*protocol.ConsumerInfoReplyPayload)
		if !ok || info.Offset() != offset+1 {
			t.Errorf("ConsumerInfo(subscriber) after ACK(%d) = %+v, want saved offset %d", offset+1, data.Payload(), offset+1)
		}
	}
	c.cancel()
	if res := c.wait(t); res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after delivery, ACK and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestListenDisconnectsConsumer checks immediate name reuse and metadata preservation over IPC.
func TestListenDisconnectsConsumer(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	c := startCall(t, t.Context(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})
	request := func(opcode protocol.Opcode, payload protocol.Payload) {
		t.Helper()
		data, err := protocol.NewData(opcode, protocol.NewClientMetadata(inbound.IPCPath()), payload)
		if err != nil {
			t.Fatalf("NewData(%v) error = %v, want nil", opcode, err)
		}
		frame, err := protocol.Encode(data)
		if err != nil {
			t.Fatalf("Encode(%v) error = %v, want nil", opcode, err)
		}
		send(t, app.inbound.IPCPath(), string(frame))
	}
	request(protocol.CreateTopic, protocol.NewCreateTopicPayload("events"))
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	offset := uint32(42)
	request(protocol.TopicSubscription, protocol.NewTopicSubscriptionPayload("subscriber", "events", "user/", &offset))
	receiveServerReply(t, inbound, protocol.TopicSubscriptionReply)
	request(protocol.ConsumerDisconnect, protocol.NewConsumerDisconnectPayload("subscriber"))
	request(protocol.TopicSubscription, protocol.NewTopicSubscriptionPayload("subscriber", "events", "user/", nil))
	receiveServerReply(t, inbound, protocol.TopicSubscriptionReply)
	request(protocol.ConsumerInfo, protocol.NewConsumerInfoPayload("subscriber"))
	data := receiveServerReply(t, inbound, protocol.ConsumerInfoReply)
	info, ok := data.Payload().(*protocol.ConsumerInfoReplyPayload)
	if !ok || info.Client() != "subscriber" || info.Topic() != "events" || info.Offset() != offset || info.Prefix() != "user/" || info.IPC() != inbound.IPCPath() {
		t.Errorf("ConsumerInfo(subscriber) after disconnect and resubscribe = %+v, want events, offset %d, user/ and IPC %q", data.Payload(), offset, inbound.IPCPath())
	}
	c.cancel()
	if res := c.wait(t); res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after disconnect, resubscribe and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestListenRejectsSubscriptionForMissingTopic checks subscription routing over IPC.
func TestListenRejectsSubscriptionForMissingTopic(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	c := startCall(t, t.Context(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})
	data, err := protocol.NewData(protocol.TopicSubscription, protocol.NewClientMetadata(inbound.IPCPath()),
		protocol.NewTopicSubscriptionPayload("consumer-1", "events", "user/", nil))
	if err != nil {
		t.Fatalf("NewData(TopicSubscription, consumer-1, events) error = %v, want nil", err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(TopicSubscription, consumer-1, events) error = %v, want nil", err)
	}
	send(t, app.inbound.IPCPath(), string(frame))
	data = receiveServerReply(t, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok || reply.Kind() != protocol.TopicNotFound || reply.Reason() == "" {
		t.Errorf("subscription to missing events payload = %+v, want %v with a nonempty reason", data.Payload(), protocol.TopicNotFound)
	}
	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	c.cancel()
	if res := c.wait(t); res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after rejected subscription and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestListenChecksCurrentTopicState checks existence before and after creation over IPC.
func TestListenChecksCurrentTopicState(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	c := startCall(t, t.Context(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})

	sendCheckRequest(t, app, inbound.IPCPath(), "events")
	data := receiveServerReply(t, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok {
		t.Errorf("CheckTopic(events) before creation payload = %T, want *protocol.ErrorReplyPayload", data.Payload())
	} else if reply.Kind() != protocol.TopicNotFound || reply.Reason() == "" {
		t.Errorf("CheckTopic(events) before creation rejection = %v, %q, want %v and a nonempty reason", reply.Kind(), reply.Reason(), protocol.TopicNotFound)
	}

	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	sendCheckRequest(t, app, inbound.IPCPath(), "events")
	data = receiveServerReply(t, inbound, protocol.CheckTopicReply)
	if _, ok := data.Payload().(*protocol.CheckTopicReplyPayload); !ok {
		t.Errorf("CheckTopic(events) after creation payload = %T, want *protocol.CheckTopicReplyPayload", data.Payload())
	}

	c.cancel()
	if res := c.wait(t); res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after topic checks and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestListenPreservesTopicState checks duplicate rejection across successive client requests.
func TestListenPreservesTopicState(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	c := startCall(t, t.Context(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})

	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	data := receiveServerReply(t, inbound, protocol.CreateTopicReply)
	if _, ok := data.Payload().(*protocol.CreateTopicReplyPayload); !ok {
		t.Errorf("first CreateTopic(events) reply payload = %T, want *protocol.CreateTopicReplyPayload", data.Payload())
	}

	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	data = receiveServerReply(t, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok {
		t.Errorf("duplicate CreateTopic(events) reply payload = %T, want *protocol.ErrorReplyPayload", data.Payload())
	} else if reply.Kind() != protocol.TopicExists {
		t.Errorf("duplicate CreateTopic(events) rejection = %v, want %v", reply.Kind(), protocol.TopicExists)
	}

	sendCreateRequest(t, app, inbound.IPCPath(), "orders")
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	c.cancel()
	res := c.wait(t)
	if res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after creation requests and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestListenKeepsExistingTopicState checks that starting the listener preserves registered topics.
func TestListenKeepsExistingTopicState(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	if err := app.topicManager.CreateTopic(ctx, ctx, lifecycle.GracefulShutdown{}, metadata, *protocol.NewCreateTopicPayload("events")); err != nil {
		t.Fatalf("CreateTopic(events) before listen() error = %v, want nil", err)
	}
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	c := startCall(t, ctx, "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})

	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	data := receiveServerReply(t, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok {
		t.Errorf("CreateTopic(events) registered before listen() reply payload = %T, want *protocol.ErrorReplyPayload", data.Payload())
	} else if reply.Kind() != protocol.TopicExists {
		t.Errorf("CreateTopic(events) registered before listen() rejection = %v, want %v", reply.Kind(), protocol.TopicExists)
	}
	c.cancel()
	res := c.wait(t)
	if res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() with existing topic state after cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestListenContinuesAfterReplyTimeout checks that a full client queue cannot stop dispatch.
func TestListenContinuesAfterReplyTimeout(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	queued := fillServerClientInbox(t, inbound)
	stderr := captureStderr(t)
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	c := startCall(t, ctx, "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})

	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	stderr.waitFor(t, context.DeadlineExceeded.Error())
	for i := 0; i < queued; i++ {
		receiveServerReply(t, inbound, protocol.ServerShutdown)
	}
	sendCreateRequest(t, app, inbound.IPCPath(), "events")
	data := receiveServerReply(t, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok {
		t.Errorf("CreateTopic(events) after a reply timeout payload = %T, want *protocol.ErrorReplyPayload", data.Payload())
	} else if reply.Kind() != protocol.TopicExists {
		t.Errorf("CreateTopic(events) after a reply timeout rejection = %v, want %v", reply.Kind(), protocol.TopicExists)
	}
	sendCreateRequest(t, app, inbound.IPCPath(), "orders")
	receiveServerReply(t, inbound, protocol.CreateTopicReply)
	c.cancel()
	res := c.wait(t)
	if res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after a reply timeout, valid requests and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

// TestListenContinuesAfterHandlerError checks recovery when a client reply cannot be sent.
func TestListenContinuesAfterHandlerError(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	stderr := captureStderr(t)
	c := startCall(t, t.Context(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.GracefulShutdown{})
	})

	sendCreateRequest(t, app, "/invalid/queue", "events")
	stderr.waitFor(t, mqueue.ErrInvalidName.Error())
	sendCreateRequest(t, app, inbound.IPCPath(), "orders")
	receiveServerReply(t, inbound, protocol.CreateTopicReply)

	c.cancel()
	res := c.wait(t)
	if res.Code() != exitcode.Success || !errors.Is(res.Err(), context.Canceled) {
		t.Errorf("listen() after a reply failure, a valid request and cancellation = %d, %v, want %d and %v", res.Code(), res.Err(), exitcode.Success, context.Canceled)
	}
}

func shutdownClientInbox(t *testing.T, suffix string) *ipc.Inbound {
	t.Helper()
	path := fmt.Sprintf("/shutdown.%d.%s.%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "."), suffix)
	inbound, err := ipc.NewInbound(path, protocol.Decode)
	if err != nil {
		t.Fatalf("NewInbound(%q) error = %v, want nil", path, err)
	}
	t.Cleanup(func() {
		if err := inbound.Drop(); err != nil {
			t.Errorf("Drop(%q) by client after server shutdown error = %v, want nil", path, err)
		}
		if err := tryConnect(t, path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("NewSender(%q) after client cleanup error = %v, want %v", path, err, os.ErrNotExist)
		}
	})
	return inbound
}

func sendShutdownRequest(t *testing.T, app *application, clientIPC string, opcode protocol.Opcode, payload protocol.Payload) {
	t.Helper()
	data, err := protocol.NewData(opcode, protocol.NewClientMetadata(clientIPC), payload)
	if err != nil {
		t.Fatalf("NewData(%v) error = %v, want nil", opcode, err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(%v) error = %v, want nil", opcode, err)
	}
	send(t, app.inbound.IPCPath(), string(frame))
}

// TestRunShutdownNotifiesActiveConsumers checks notification and client ownership of queues.
func TestRunShutdownNotifiesActiveConsumers(t *testing.T) {
	app := newTestApplication(t)
	requested := make(chan struct{})
	c := startCall(t, t.Context(), "run()", func(ctx context.Context) (int, error) {
		return app.run(ctx, lifecycle.NewGracefulShutdown(requested))
	})
	producer := newServerClientInbox(t)
	sendCreateRequest(t, app, producer.IPCPath(), "events")
	receiveServerReply(t, producer, protocol.CreateTopicReply)
	consumers := []*ipc.Inbound{shutdownClientInbox(t, "first"), shutdownClientInbox(t, "second")}
	for i, inbound := range consumers {
		sendShutdownRequest(t, app, inbound.IPCPath(), protocol.TopicSubscription,
			protocol.NewTopicSubscriptionPayload(fmt.Sprintf("subscriber-%d", i), "events", "", nil))
		receiveServerReply(t, inbound, protocol.TopicSubscriptionReply)
	}
	started := time.Now()
	close(requested)
	for _, inbound := range consumers {
		receiveServerReply(t, inbound, protocol.ServerShutdown)
	}
	res := c.wait(t)
	if res.Code() != exitcode.Success || res.Err() != nil {
		t.Errorf("run() with two active consumers = %d, %v, want %d and nil", res.Code(), res.Err(), exitcode.Success)
	}
	if elapsed := time.Since(started); elapsed >= gracePeriod {
		t.Errorf("shutdown with no pending ACK took %v, want less than %v", elapsed, gracePeriod)
	}
	if !app.readyToFinish() {
		t.Error("readyToFinish() after shutdown = false, want true")
	}
	for _, inbound := range consumers {
		if err := tryConnect(t, inbound.IPCPath()); err != nil {
			t.Errorf("NewSender(%q) after shutdown error = %v, want nil until client cleanup", inbound.IPCPath(), err)
		}
	}
}

// TestRunShutdownWaitsForACKOrDeadline checks early completion and the five-second bound.
func TestRunShutdownWaitsForACKOrDeadline(t *testing.T) {
	for _, ack := range []bool{true, false} {
		t.Run(fmt.Sprintf("ack_%t", ack), func(t *testing.T) {
			app := newTestApplication(t)
			requested := make(chan struct{})
			c := startCall(t, t.Context(), "run()", func(ctx context.Context) (int, error) {
				return app.run(ctx, lifecycle.NewGracefulShutdown(requested))
			})
			producer := newServerClientInbox(t)
			inbound := shutdownClientInbox(t, "consumer")
			sendCreateRequest(t, app, producer.IPCPath(), "events")
			receiveServerReply(t, producer, protocol.CreateTopicReply)
			sendShutdownRequest(t, app, producer.IPCPath(), protocol.ProduceMessage,
				protocol.NewProduceMessagePayload("events", protocol.NewMessage([]byte("key"), []byte("value"))))
			receiveServerReply(t, producer, protocol.ProduceMessageReply)
			sendShutdownRequest(t, app, inbound.IPCPath(), protocol.TopicSubscription,
				protocol.NewTopicSubscriptionPayload("subscriber", "events", "", nil))
			receiveServerReply(t, inbound, protocol.TopicSubscriptionReply)
			receiveServerReply(t, inbound, protocol.MessageDelivery)
			started := time.Now()
			close(requested)
			receiveServerReply(t, inbound, protocol.ServerShutdown)
			if app.readyToFinish() {
				t.Error("readyToFinish() with ACK(1) pending = true, want false")
			}
			sendShutdownRequest(t, app, producer.IPCPath(), protocol.ProduceMessage,
				protocol.NewProduceMessagePayload("events", protocol.NewMessage(nil, nil)))
			receiveServerReply(t, producer, protocol.ServerShutdown)
			sendShutdownRequest(t, app, producer.IPCPath(), protocol.TopicSubscription,
				protocol.NewTopicSubscriptionPayload("new-subscriber", "events", "", nil))
			receiveServerReply(t, producer, protocol.ServerShutdown)
			var offset uint32
			if ack {
				offset = 1
				sendShutdownRequest(t, app, inbound.IPCPath(), protocol.Acknowledgement,
					protocol.NewAcknowledgementPayload("subscriber", offset))
				c.wait(t)
			} else {
				select {
				case c.result = <-c.done:
					c.returned = true
				case <-time.After(gracePeriod + 2*shutdownPollInterval):
					t.Fatalf("run() with an unacknowledged delivery did not stop within %v", gracePeriod+2*shutdownPollInterval)
				}
			}
			elapsed := time.Since(started)
			if ack && elapsed >= gracePeriod {
				t.Errorf("shutdown after ACK took %v, want less than %v", elapsed, gracePeriod)
			} else if !ack && elapsed < gracePeriod {
				t.Errorf("forced shutdown without ACK took %v, want at least %v", elapsed, gracePeriod)
			}
			if c.result.Code() != exitcode.Success || c.result.Err() != nil {
				t.Errorf("run() with ack=%t = %d, %v, want %d and nil", ack, c.result.Code(), c.result.Err(), exitcode.Success)
			}
			if !app.readyToFinish() {
				t.Error("readyToFinish() after run returned = false, want true")
			}
			ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
			defer cancel()
			if err := app.topicManager.ConsumerInfo(ctx, *protocol.NewClientMetadata(producer.IPCPath()), *protocol.NewConsumerInfoPayload("subscriber")); err != nil {
				t.Fatalf("ConsumerInfo(subscriber) after shutdown error = %v, want nil", err)
			}
			data := receiveServerReply(t, producer, protocol.ConsumerInfoReply)
			info, ok := data.Payload().(*protocol.ConsumerInfoReplyPayload)
			if !ok || info.Offset() != offset {
				t.Errorf("ConsumerInfo after shutdown with ack=%t = %#v, want offset %d", ack, data.Payload(), offset)
			}
			if err := tryConnect(t, inbound.IPCPath()); err != nil {
				t.Errorf("NewSender(consumer) after shutdown error = %v, want nil until client cleanup", err)
			}
		})
	}
}
