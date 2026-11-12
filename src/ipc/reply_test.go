package ipc

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

// TestReplySendsServerFrame checks the envelope and payload delivered to the client.
func TestReplySendsServerFrame(t *testing.T) {
	ctx := testContext(t)
	inbound := newTestInbound(t, testIPCPath(t), protocol.Decode)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	if err := Reply(ctx, metadata, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload()); err != nil {
		t.Errorf("Reply(%q, CreateTopicReply) error = %v, want nil", inbound.IPCPath(), err)
		return
	}
	data, err := inbound.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv(%q) after Reply(CreateTopicReply) error = %v, want nil", inbound.IPCPath(), err)
	}
	if data.Emitter() != protocol.Server || data.Opcode() != protocol.CreateTopicReply {
		t.Errorf("Reply(%q, CreateTopicReply) envelope = emitter %v, opcode %v, want %v and %v", inbound.IPCPath(), data.Emitter(), data.Opcode(), protocol.Server, protocol.CreateTopicReply)
	}
	if _, ok := data.Metadata().(*protocol.ServerMetadata); !ok {
		t.Errorf("Reply(%q, CreateTopicReply) metadata = %T, want *protocol.ServerMetadata", inbound.IPCPath(), data.Metadata())
	}
	if _, ok := data.Payload().(*protocol.CreateTopicReplyPayload); !ok {
		t.Errorf("Reply(%q, CreateTopicReply) payload = %T, want *protocol.CreateTopicReplyPayload", inbound.IPCPath(), data.Payload())
	}
}

// TestReplyRejectsInvalidPayloads checks validation before opening a client queue.
func TestReplyRejectsInvalidPayloads(t *testing.T) {
	metadata := *protocol.NewClientMetadata("/missing")
	err := Reply(testContext(t), metadata, protocol.CreateTopicReply, nil)
	if !errors.Is(err, protocol.ErrMissingPayload) {
		t.Errorf("Reply(CreateTopicReply, nil) error = %v, want %v", err, protocol.ErrMissingPayload)
	}
	if errors.Is(err, ErrReply) {
		t.Errorf("Reply(CreateTopicReply, nil) error = %v, want a validation error without %v", err, ErrReply)
	}
}

// TestReplyPropagatesQueueErrors checks that connection failures remain distinct from sends.
func TestReplyPropagatesQueueErrors(t *testing.T) {
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
			err := Reply(testContext(t), metadata, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload())
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Reply(%q, CreateTopicReply) error = %v, want %v", tt.ipcPath, err, tt.wantErr)
			}
			if errors.Is(err, ErrReply) {
				t.Errorf("Reply(%q, CreateTopicReply) error = %v, want a connection error without %v", tt.ipcPath, err, ErrReply)
			}
		})
	}
}

// TestReplyPropagatesStoppedContexts checks cancellation and deadline identities.
func TestReplyPropagatesStoppedContexts(t *testing.T) {
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
			err := Reply(tt.ctx, metadata, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload())
			if !errors.Is(err, tt.wantErr) || !errors.Is(err, ErrReply) {
				t.Errorf("Reply(CreateTopicReply) with %s context error = %v, want %v and %v", tt.name, err, tt.wantErr, ErrReply)
			}
		})
	}
}

// TestReplyTimesOutOnFullQueue checks that a pending send respects its deadline.
func TestReplyTimesOutOnFullQueue(t *testing.T) {
	inbound := newTestInbound(t, testIPCPath(t), protocol.Decode)
	fillInbound(t, inbound)
	ctx, cancel := context.WithTimeout(testContext(t), 20*time.Millisecond)
	defer cancel()
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	err := Reply(ctx, metadata, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload())
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrReply) {
		t.Errorf("Reply(%q, CreateTopicReply) with a full queue error = %v, want %v and %v", inbound.IPCPath(), err, context.DeadlineExceeded, ErrReply)
	}
}

// TestReplyPropagatesSendErrors checks oversized frames after a queue has been opened.
func TestReplyPropagatesSendErrors(t *testing.T) {
	inbound := newTestInbound(t, testIPCPath(t), protocol.Decode)
	metadata := *protocol.NewClientMetadata(inbound.IPCPath())
	topics := make([]string, 1024)
	for i := range topics {
		topics[i] = "events"
	}
	err := Reply(testContext(t), metadata, protocol.ListTopicReply, protocol.NewListTopicReplyPayload(topics))
	if !errors.Is(err, mqueue.ErrFrameTooLarge) || !errors.Is(err, ErrReply) {
		t.Errorf("Reply(ListTopicReply, %d topics) error = %v, want %v and %v", len(topics), err, mqueue.ErrFrameTooLarge, ErrReply)
	}
}
