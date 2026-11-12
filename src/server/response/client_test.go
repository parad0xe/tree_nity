package response

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

func testIPCPath(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("/server-response.%d.%s", os.Getpid(), strings.ReplaceAll(t.Name(), "/", "."))
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newTestInbound(t *testing.T, ipcPath string, decode ipc.DecodeFunc) *ipc.Inbound {
	t.Helper()
	inbound, err := ipc.NewInbound(ipcPath, decode)
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

func fillInbound(t *testing.T, inbound *ipc.Inbound) {
	t.Helper()
	sender, err := mqueue.NewSender(inbound.IPCPath())
	if err != nil {
		t.Fatalf("NewSender(%q) error = %v, want nil", inbound.IPCPath(), err)
	}
	t.Cleanup(func() {
		if err := sender.Close(); err != nil {
			t.Errorf("Close(%q) error = %v, want nil", inbound.IPCPath(), err)
		}
	})
	frame := []byte("frame")
	for {
		err := sender.TimedSend(frame, time.Now())
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return
		}
		if err != nil {
			t.Fatalf("TimedSend(%q) to %q error = %v, want nil or %v", frame, inbound.IPCPath(), err, os.ErrDeadlineExceeded)
		}
	}
}

// TestSendToClientSendsServerFrame checks the envelope and payload delivered to the client.
func TestSendToClientSendsServerFrame(t *testing.T) {
	ctx := testContext(t)
	inbound := newTestInbound(t, testIPCPath(t), protocol.Decode)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	if err := SendToClient(ctx, metadata, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload()); err != nil {
		t.Errorf("SendToClient(%q, CreateTopicReply) error = %v, want nil", inbound.IPCPath(), err)
		return
	}
	data, err := inbound.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv(%q) after SendToClient(CreateTopicReply) error = %v, want nil", inbound.IPCPath(), err)
	}
	if data.Emitter() != protocol.Server || data.Opcode() != protocol.CreateTopicReply {
		t.Errorf("SendToClient(%q, CreateTopicReply) envelope = emitter %v, opcode %v, want %v and %v", inbound.IPCPath(), data.Emitter(), data.Opcode(), protocol.Server, protocol.CreateTopicReply)
	}
	if _, ok := data.Metadata().(*protocol.ServerMetadata); !ok {
		t.Errorf("SendToClient(%q, CreateTopicReply) metadata = %T, want *protocol.ServerMetadata", inbound.IPCPath(), data.Metadata())
	}
	if _, ok := data.Payload().(*protocol.CreateTopicReplyPayload); !ok {
		t.Errorf("SendToClient(%q, CreateTopicReply) payload = %T, want *protocol.CreateTopicReplyPayload", inbound.IPCPath(), data.Payload())
	}
}

// TestSendToClientRejectsInvalidPayloads checks validation before opening a client queue.
func TestSendToClientRejectsInvalidPayloads(t *testing.T) {
	metadata := *protocol.NewClientMetadata("/missing")
	err := SendToClient(testContext(t), metadata, protocol.CreateTopicReply, nil)
	if !errors.Is(err, protocol.ErrMissingPayload) {
		t.Errorf("SendToClient(CreateTopicReply, nil) error = %v, want %v", err, protocol.ErrMissingPayload)
	}
	if errors.Is(err, ErrReply) {
		t.Errorf("SendToClient(CreateTopicReply, nil) error = %v, want a validation error without %v", err, ErrReply)
	}
}

// TestSendToClientPropagatesQueueErrors checks that connection failures remain distinct from sends.
func TestSendToClientPropagatesQueueErrors(t *testing.T) {
	tests := []struct {
		name    string
		ipcPath string
		wantErr error
	}{
		{name: "invalid name", ipcPath: "/invalid/queue", wantErr: mqueue.ErrInvalidName},
		{name: "missing queue", ipcPath: testIPCPath(t), wantErr: os.ErrNotExist},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := *protocol.NewClientMetadata(tt.ipcPath)
			err := SendToClient(testContext(t), metadata, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload())
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("SendToClient(%q, CreateTopicReply) error = %v, want %v", tt.ipcPath, err, tt.wantErr)
			}
			if errors.Is(err, ErrReply) {
				t.Errorf("SendToClient(%q, CreateTopicReply) error = %v, want a connection error without %v", tt.ipcPath, err, ErrReply)
			}
		})
	}
}

// TestSendToClientPropagatesStoppedContexts checks cancellation and deadline identities.
func TestSendToClientPropagatesStoppedContexts(t *testing.T) {
	inbound := newTestInbound(t, testIPCPath(t), protocol.Decode)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, expireCancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer expireCancel()
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{name: "canceled", ctx: canceled, wantErr: context.Canceled},
		{name: "expired", ctx: expired, wantErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SendToClient(tt.ctx, metadata, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload())
			if !errors.Is(err, tt.wantErr) || !errors.Is(err, ErrReply) {
				t.Errorf("SendToClient(CreateTopicReply) with %s context error = %v, want %v and %v", tt.name, err, tt.wantErr, ErrReply)
			}
		})
	}
}

// TestSendToClientTimesOutOnFullQueue checks that a pending send respects its deadline.
func TestSendToClientTimesOutOnFullQueue(t *testing.T) {
	inbound := newTestInbound(t, testIPCPath(t), protocol.Decode)
	fillInbound(t, inbound)
	ctx, cancel := context.WithTimeout(testContext(t), 20*time.Millisecond)
	defer cancel()
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	err := SendToClient(ctx, metadata, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload())
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrReply) {
		t.Errorf("SendToClient(%q, CreateTopicReply) with a full queue error = %v, want %v and %v", inbound.IPCPath(), err, context.DeadlineExceeded, ErrReply)
	}
}

// TestSendToClientPropagatesSendErrors checks oversized frames after a queue has been opened.
func TestSendToClientPropagatesSendErrors(t *testing.T) {
	inbound := newTestInbound(t, testIPCPath(t), protocol.Decode)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	topics := make([]string, 1024)
	for i := range topics {
		topics[i] = "events"
	}
	err := SendToClient(testContext(t), metadata, protocol.ListTopicReply, protocol.NewListTopicReplyPayload(topics))
	if !errors.Is(err, mqueue.ErrFrameTooLarge) || !errors.Is(err, ErrReply) {
		t.Errorf("SendToClient(ListTopicReply, %d topics) error = %v, want %v and %v", len(topics), err, mqueue.ErrFrameTooLarge, ErrReply)
	}
}
