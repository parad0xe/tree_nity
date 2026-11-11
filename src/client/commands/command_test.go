package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/protocol"
)

// testTimeout is the longest a test waits for an event before it fails.
const testTimeout = 5 * time.Second

// errTestWrite is the error that failingWriter returns.
var errTestWrite = errors.New("test write failure")

// failingWriter is a mock writer whose writes always fail.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errTestWrite
}

// testIPCPath returns a queue name unique to the test and to the role.
func testIPCPath(t *testing.T, role string) string {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", ".")
	return fmt.Sprintf("/test.commands.%s.%s.%d", role, name, os.Getpid())
}

// newTestInbound creates the queue ipcPath, and drops it when the test ends.
func newTestInbound(t *testing.T, ipcPath string) *ipc.Inbound {
	t.Helper()
	in, err := ipc.NewInbound(ipcPath, protocol.Decode)
	if err != nil {
		t.Fatalf("NewInbound(%q) error = %v, want nil", ipcPath, err)
	}
	t.Cleanup(func() {
		if err := in.Drop(); err != nil {
			t.Errorf("Drop() error = %v, want nil", err)
		}
	})
	return in
}

// newTestOutbound opens the queue ipcPath, and closes it when the test ends.
func newTestOutbound(t *testing.T, ipcPath string) *ipc.Outbound {
	t.Helper()
	out, err := ipc.NewOutbound(ipcPath, protocol.Encode)
	if err != nil {
		t.Fatalf("NewOutbound(%q) error = %v, want nil", ipcPath, err)
	}
	t.Cleanup(func() {
		if err := out.Close(); err != nil {
			t.Errorf("Close() error = %v, want nil", err)
		}
	})
	return out
}

// newClientData returns a frame sent by a client.
func newClientData(t *testing.T, opcode protocol.Opcode, payload protocol.Payload) protocol.Data {
	t.Helper()
	data, err := protocol.NewData(opcode, protocol.NewClientMetadata("/client"), payload)
	if err != nil {
		t.Fatalf("NewData(%d) for a client error = %v, want nil", opcode, err)
	}
	return data
}

// newServerData returns a frame sent by the server.
func newServerData(t *testing.T, opcode protocol.Opcode, payload protocol.Payload) protocol.Data {
	t.Helper()
	data, err := protocol.NewData(opcode, protocol.NewServerMetadata(), payload)
	if err != nil {
		t.Fatalf("NewData(%d) for the server error = %v, want nil", opcode, err)
	}
	return data
}

// newReplyEnv returns an Env whose inbound already holds the reply.
func newReplyEnv(t *testing.T, reply protocol.Data) Env {
	t.Helper()
	inbound := newTestInbound(t, testIPCPath(t, "client"))
	server := newTestOutbound(t, inbound.IPCPath())
	if err := server.Send(context.Background(), reply); err != nil {
		t.Fatalf("Send(%d) error = %v, want nil", reply.Opcode(), err)
	}
	return Env{Inbound: inbound, RecvTimeout: testTimeout, SendTimeout: testTimeout}
}

// recvTestData returns the next frame of the queue, waiting at most testTimeout.
func recvTestData(t *testing.T, in *ipc.Inbound) protocol.Data {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	data, err := in.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv() error = %v, want nil", err)
	}
	return data
}

// cancelledContext returns a context that is already cancelled.
func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestCreateDataSetsClientQueue(t *testing.T) {
	env := Env{Inbound: newTestInbound(t, testIPCPath(t, "client"))}
	payload := protocol.NewCreateTopicPayload("orders")

	got, err := env.createData(protocol.CreateTopic, payload)

	if err != nil {
		t.Fatalf("createData(%d) error = %v, want nil", protocol.CreateTopic, err)
	}
	want, err := protocol.NewData(protocol.CreateTopic, protocol.NewClientMetadata(env.Inbound.IPCPath()), payload)
	if err != nil {
		t.Fatalf("NewData(%d) error = %v, want nil", protocol.CreateTopic, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("createData(%d) = %+v, want %+v", protocol.CreateTopic, got, want)
	}
}

func TestCreateDataRejectsInvalidPayload(t *testing.T) {
	env := Env{Inbound: newTestInbound(t, testIPCPath(t, "client"))}
	payload := protocol.NewCreateTopicPayload("")

	if _, err := env.createData(protocol.CreateTopic, payload); !errors.Is(err, protocol.ErrInvalidPayload) {
		t.Errorf("createData(%d) with an empty topic error = %v, want %v", protocol.CreateTopic, err, protocol.ErrInvalidPayload)
	}
}

func TestSendDeliversData(t *testing.T) {
	server := newTestInbound(t, testIPCPath(t, "server"))
	env := Env{Outbound: newTestOutbound(t, server.IPCPath()), SendTimeout: testTimeout}
	data := newClientData(t, protocol.CreateTopic, protocol.NewCreateTopicPayload("orders"))

	code, err := env.send(context.Background(), data)

	if err != nil || code != exitcode.Success {
		t.Fatalf("send(%d) = %d, %v, want %d, nil", data.Opcode(), code, err, exitcode.Success)
	}
	if got := recvTestData(t, server); !reflect.DeepEqual(got, data) {
		t.Errorf("Recv() after send(%d) = %+v, want %+v", data.Opcode(), got, data)
	}
}

func TestSendReturnsIPCCodeOnFailure(t *testing.T) {
	server := newTestInbound(t, testIPCPath(t, "server"))
	env := Env{Outbound: newTestOutbound(t, server.IPCPath()), SendTimeout: testTimeout}
	data := newClientData(t, protocol.CreateTopic, protocol.NewCreateTopicPayload("orders"))

	code, err := env.send(cancelledContext(), data)

	if code != exitcode.IPC || !errors.Is(err, context.Canceled) {
		t.Errorf("send(%d) with a cancelled context = %d, %v, want %d, %v", data.Opcode(), code, err, exitcode.IPC, context.Canceled)
	}
}

func TestRecvReturnsExpectedReply(t *testing.T) {
	want := protocol.NewCreateTopicReplyPayload()
	env := newReplyEnv(t, newServerData(t, protocol.CreateTopicReply, want))

	got, code, err := env.recv[*protocol.CreateTopicReplyPayload](context.Background(), protocol.CreateTopicReply)

	if err != nil || code != exitcode.Success {
		t.Fatalf("recv(%d) = %d, %v, want %d, nil", protocol.CreateTopicReply, code, err, exitcode.Success)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("recv(%d) payload = %+v, want %+v", protocol.CreateTopicReply, got, want)
	}
}

func TestRecvRejectsUnexpectedEmitter(t *testing.T) {
	env := newReplyEnv(t, newClientData(t, protocol.CreateTopic, protocol.NewCreateTopicPayload("orders")))

	_, code, err := env.recv[*protocol.CreateTopicReplyPayload](context.Background(), protocol.CreateTopicReply)

	if code != exitcode.General || !errors.Is(err, errUnexpectedEmitter) {
		t.Errorf("recv(%d) of a client frame = %d, %v, want %d, %v", protocol.CreateTopicReply, code, err, exitcode.General, errUnexpectedEmitter)
	}
}

func TestRecvReturnsGeneralCodeForInvalidRequest(t *testing.T) {
	reply := protocol.NewErrorReplyPayload(protocol.InvalidRequest, "invalid request")
	env := newReplyEnv(t, newServerData(t, protocol.ErrorReply, reply))

	_, code, err := env.recv[*protocol.CreateTopicReplyPayload](context.Background(), protocol.CreateTopicReply)

	if code != exitcode.General || !errors.Is(err, errRequestRejected) {
		t.Errorf("recv(%d) of an error reply with kind %d = %d, %v, want %d, %v", protocol.CreateTopicReply, reply.Kind(), code, err, exitcode.General, errRequestRejected)
	}
}

func TestRecvReturnsTopicCodeForRejection(t *testing.T) {
	tests := []struct {
		name string
		kind protocol.ErrorKind
	}{
		{name: "topic not found", kind: protocol.TopicNotFound},
		{name: "topic exists", kind: protocol.TopicExists},
		{name: "client not found", kind: protocol.ClientNotFound},
		{name: "client name taken", kind: protocol.ClientNameTaken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reply := protocol.NewErrorReplyPayload(tt.kind, tt.name)
			env := newReplyEnv(t, newServerData(t, protocol.ErrorReply, reply))

			_, code, err := env.recv[*protocol.CreateTopicReplyPayload](context.Background(), protocol.CreateTopicReply)

			if code != exitcode.Topic || !errors.Is(err, errRequestRejected) {
				t.Errorf("recv(%d) of an error reply with kind %d = %d, %v, want %d, %v", protocol.CreateTopicReply, tt.kind, code, err, exitcode.Topic, errRequestRejected)
			}
		})
	}
}

func TestRecvRejectsUnexpectedOpcode(t *testing.T) {
	env := newReplyEnv(t, newServerData(t, protocol.ListTopicReply, protocol.NewListTopicReplyPayload(nil)))

	_, code, err := env.recv[*protocol.CreateTopicReplyPayload](context.Background(), protocol.CreateTopicReply)

	if code != exitcode.General || !errors.Is(err, errUnexpectedOpcode) {
		t.Errorf("recv(%d) of opcode %d = %d, %v, want %d, %v", protocol.CreateTopicReply, protocol.ListTopicReply, code, err, exitcode.General, errUnexpectedOpcode)
	}
}

func TestRecvRejectsUnexpectedPayload(t *testing.T) {
	env := newReplyEnv(t, newServerData(t, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload()))

	_, code, err := env.recv[*protocol.ListTopicReplyPayload](context.Background(), protocol.CreateTopicReply)

	if code != exitcode.General || !errors.Is(err, errUnexpectedPayload) {
		t.Errorf("recv[*ListTopicReplyPayload](%d) = %d, %v, want %d, %v", protocol.CreateTopicReply, code, err, exitcode.General, errUnexpectedPayload)
	}
}

func TestRecvReturnsIPCCodeWithoutReply(t *testing.T) {
	env := Env{Inbound: newTestInbound(t, testIPCPath(t, "client")), RecvTimeout: testTimeout}

	_, code, err := env.recv[*protocol.CreateTopicReplyPayload](cancelledContext(), protocol.CreateTopicReply)

	if code != exitcode.IPC || !errors.Is(err, context.Canceled) {
		t.Errorf("recv(%d) with a cancelled context = %d, %v, want %d, %v", protocol.CreateTopicReply, code, err, exitcode.IPC, context.Canceled)
	}
}

func TestPrintWritesBytesAsIs(t *testing.T) {
	var stdout bytes.Buffer
	env := Env{Stdout: &stdout}
	b := []byte("50%d\x00\xff")

	if err := env.print(b); err != nil {
		t.Fatalf("print(%q) error = %v, want nil", b, err)
	}
	if got := stdout.Bytes(); !bytes.Equal(got, b) {
		t.Errorf("print(%q) wrote %q, want %q", b, got, b)
	}
}

func TestPrintReturnsWriteError(t *testing.T) {
	env := Env{Stdout: failingWriter{}}
	b := []byte("orders")

	if err := env.print(b); !errors.Is(err, errTestWrite) {
		t.Errorf("print(%q) error = %v, want %v", b, err, errTestWrite)
	}
}

func TestPrintlnWritesLine(t *testing.T) {
	var stdout bytes.Buffer
	env := Env{Stdout: &stdout}
	s := "orders"

	if err := env.println(s); err != nil {
		t.Fatalf("println(%q) error = %v, want nil", s, err)
	}
	if got, want := stdout.String(), s+"\n"; got != want {
		t.Errorf("println(%q) wrote %q, want %q", s, got, want)
	}
}

func TestPrintlnReturnsWriteError(t *testing.T) {
	env := Env{Stdout: failingWriter{}}
	s := "orders"

	if err := env.println(s); !errors.Is(err, errTestWrite) {
		t.Errorf("println(%q) error = %v, want %v", s, err, errTestWrite)
	}
}
