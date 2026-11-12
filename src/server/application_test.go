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
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
	"tree_nity/src/server/session/topic"
)

const (
	// testTimeout is the longest a test waits for an event before it fails.
	testTimeout = 5 * time.Second
	// validFrame decodes to protocol data whose opcode the server does not handle.
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

// TestListenRepliesToUnhandledOpcode checks rejection delivery before graceful shutdown.
func TestListenRepliesToUnhandledOpcode(t *testing.T) {
	app := newTestApplication(t)
	inbound := newServerClientInbox(t)
	stderr := captureStderr(t)
	requestedChan := make(chan struct{})
	c := startCall(t, context.Background(), "listen()", func(ctx context.Context) (int, error) {
		return app.listen(ctx, lifecycle.NewGracefulShutdown(requestedChan))
	})

	data, err := protocol.NewData(protocol.ConsumerHeartbeat, protocol.NewClientMetadata(inbound.IPCPath()), protocol.NewConsumerHeartbeatPayload("client-1"))
	if err != nil {
		t.Fatalf("NewData(ConsumerHeartbeat, %q) error = %v, want nil", inbound.IPCPath(), err)
	}
	frame, err := protocol.Encode(data)
	if err != nil {
		t.Fatalf("Encode(ConsumerHeartbeat) error = %v, want nil", err)
	}
	send(t, app.inbound.IPCPath(), string(frame))
	data = receiveServerReply(t, inbound, protocol.ErrorReply)
	reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
	if !ok {
		t.Errorf("ConsumerHeartbeat rejection payload = %T, want *protocol.ErrorReplyPayload", data.Payload())
	} else if reply.Kind() != protocol.InvalidRequest || reply.Reason() == "" {
		t.Errorf("ConsumerHeartbeat rejection = %v, %q, want %v and a nonempty reason", reply.Kind(), reply.Reason(), protocol.InvalidRequest)
	}
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
	stderr.waitFor(t, errUnhandledOpcode.Error())
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
	stderr.waitFor(t, errUnhandledOpcode.Error())
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
	app := &application{inbound: inbound}
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

// TestDispatchAcceptsTopicRequests checks routing to the existing placeholder handlers.
func TestDispatchAcceptsTopicRequests(t *testing.T) {
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
	}{
		{name: "list", opcode: protocol.ListTopic, payload: protocol.NewListTopicPayload()},
		{name: "subscribe", opcode: protocol.TopicSubscription, payload: protocol.NewTopicSubscriptionPayload("client-1", "events", "", nil)},
		{name: "check", opcode: protocol.CheckTopic, payload: protocol.NewCheckTopicPayload("events")},
		{name: "produce", opcode: protocol.ProduceMessage, payload: protocol.NewProduceMessagePayload("events", protocol.NewMessage([]byte("key"), []byte("value")))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager(t.Context())}
			data, err := protocol.NewData(tt.opcode, protocol.NewClientMetadata("/client"), tt.payload)
			if err != nil {
				t.Fatalf("NewData(%v) during setup error = %v, want nil", tt.opcode, err)
			}
			if err := app.dispatch(t.Context(), data); err != nil {
				t.Errorf("dispatch(%v, %T) error = %v, want nil", tt.opcode, tt.payload, err)
			}
		})
	}
}

// TestDispatchRejectsUnhandledRequests checks error replies for unsupported client requests.
func TestDispatchRejectsUnhandledRequests(t *testing.T) {
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
	}{
		{name: "consumer info", opcode: protocol.ConsumerInfo, payload: protocol.NewConsumerInfoPayload("client-1", "consumer-1")},
		{name: "acknowledgement", opcode: protocol.Acknowledgement, payload: protocol.NewAcknowledgementPayload("client-1", 1)},
		{name: "disconnect", opcode: protocol.ConsumerDisconnect, payload: protocol.NewConsumerDisconnectPayload("client-1")},
		{name: "heartbeat", opcode: protocol.ConsumerHeartbeat, payload: protocol.NewConsumerHeartbeatPayload("client-1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &application{topicManager: topic.NewManager(t.Context())}
			inbound := newServerClientInbox(t)
			data, err := protocol.NewData(tt.opcode, protocol.NewClientMetadata(inbound.IPCPath()), tt.payload)
			if err != nil {
				t.Fatalf("NewData(%v, %q) during setup error = %v, want nil", tt.opcode, inbound.IPCPath(), err)
			}
			if err := app.dispatch(t.Context(), data); !errors.Is(err, errUnhandledOpcode) {
				t.Errorf("dispatch(%v, %q, %T) error = %v, want %v", tt.opcode, inbound.IPCPath(), tt.payload, err, errUnhandledOpcode)
			}
			data = receiveServerReply(t, inbound, protocol.ErrorReply)
			reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
			if !ok {
				t.Errorf("dispatch(%v) rejection payload = %T, want *protocol.ErrorReplyPayload", tt.opcode, data.Payload())
			} else if reply.Kind() != protocol.InvalidRequest || reply.Reason() == "" {
				t.Errorf("dispatch(%v) rejection = %v, %q, want %v and a nonempty reason", tt.opcode, reply.Kind(), reply.Reason(), protocol.InvalidRequest)
			}
		})
	}
}

// TestDispatchRejectsServerFrames checks rejection of server opcodes overlapping client opcodes.
func TestDispatchRejectsServerFrames(t *testing.T) {
	app := &application{topicManager: topic.NewManager(t.Context())}
	data, err := protocol.NewData(protocol.CreateTopicReply, protocol.NewServerMetadata(), protocol.NewCreateTopicReplyPayload())
	if err != nil {
		t.Fatalf("NewData(CreateTopicReply) during setup error = %v, want nil", err)
	}
	if err := app.dispatch(t.Context(), data); !errors.Is(err, errUnhandledOpcode) {
		t.Errorf("dispatch(CreateTopicReply) error = %v, want %v", err, errUnhandledOpcode)
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
			app := &application{topicManager: topic.NewManager(t.Context())}
			data, err := protocol.NewData(protocol.ConsumerHeartbeat, protocol.NewClientMetadata(tt.ipcPath), protocol.NewConsumerHeartbeatPayload("client-1"))
			if err != nil {
				t.Fatalf("NewData(ConsumerHeartbeat, %q) during setup error = %v, want nil", tt.ipcPath, err)
			}
			err = app.dispatch(t.Context(), data)
			if !errors.Is(err, errUnhandledOpcode) || !errors.Is(err, tt.wantErr) {
				t.Errorf("dispatch(ConsumerHeartbeat, %q) error = %v, want %v and %v", tt.ipcPath, err, errUnhandledOpcode, tt.wantErr)
			}
		})
	}
}

// TestDispatchCancelsRejection checks that reply delivery respects the listener context.
func TestDispatchCancelsRejection(t *testing.T) {
	app := &application{topicManager: topic.NewManager(t.Context())}
	inbound := newServerClientInbox(t)
	data, err := protocol.NewData(protocol.ConsumerHeartbeat, protocol.NewClientMetadata(inbound.IPCPath()), protocol.NewConsumerHeartbeatPayload("client-1"))
	if err != nil {
		t.Fatalf("NewData(ConsumerHeartbeat, %q) during setup error = %v, want nil", inbound.IPCPath(), err)
	}
	err = app.dispatch(cancelledContext(), data)
	if !errors.Is(err, errUnhandledOpcode) || !errors.Is(err, context.Canceled) {
		t.Errorf("dispatch(ConsumerHeartbeat) with a canceled context error = %v, want %v and %v", err, errUnhandledOpcode, context.Canceled)
	}
}

// TestDispatchReturnsHandlerErrors checks that IPC failures remain inspectable by the caller.
func TestDispatchReturnsHandlerErrors(t *testing.T) {
	app := &application{topicManager: topic.NewManager(t.Context())}
	data, err := protocol.NewData(protocol.CreateTopic, protocol.NewClientMetadata("/invalid/queue"), protocol.NewCreateTopicPayload("events"))
	if err != nil {
		t.Fatalf("NewData(CreateTopic, /invalid/queue, events) during setup error = %v, want nil", err)
	}
	if err := app.dispatch(t.Context(), data); !errors.Is(err, mqueue.ErrInvalidName) {
		t.Errorf("dispatch(CreateTopic, /invalid/queue, events) error = %v, want %v", err, mqueue.ErrInvalidName)
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
