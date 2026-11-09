package commands

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
)

const (
	// sendTimeout is the longest a request waits while the server queue is full.
	sendTimeout = time.Second * 2
	// recvTimeout is the longest a command waits for the server reply.
	recvTimeout = time.Second * 2
)

// Env holds what every command can use while it runs.
type Env struct {
	// Shutdown is requested after SIGINT or SIGTERM.
	Shutdown lifecycle.GracefulShutdown
	// Inbound receives what the server sends to the client.
	Inbound *ipc.Inbound
	// Outbound sends the requests of the client to the server.
	Outbound *ipc.Outbound
}

// send sends data to the server.
func (env *Env) send(ctx context.Context, data protocol.Data) (int, error) {
	sendCtx, sendCancel := context.WithTimeout(ctx, sendTimeout)
	defer sendCancel()

	if err := env.Outbound.Send(sendCtx, data); err != nil {
		return exitcode.IPC, fmt.Errorf("send: %w", err)
	}

	return exitcode.Success, nil
}

// recv returns the payload of the server reply with expectedOpcode.
func (env *Env) recv[P protocol.Payload](ctx context.Context, expectedOpcode protocol.Opcode) (P, int, error) {
	var zero P

	recvCtx, recvCancel := context.WithTimeout(ctx, recvTimeout)
	defer recvCancel()

	data, recvErr := env.Inbound.Recv(recvCtx)
	if recvErr != nil {
		return zero, exitcode.IPC, fmt.Errorf("recv: %w", recvErr)
	}

	if data.Emitter() != protocol.Server {
		return zero, exitcode.General, fmt.Errorf("recv: %w", errUnexpectedEmitter)
	}

	if data.Opcode() == protocol.ErrorReply {
		reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
		if !ok {
			return zero, exitcode.General, fmt.Errorf("recv: %w", errUnexpectedPayload)
		}

		var code int
		switch reply.Kind() {
		case protocol.InvalidRequest:
			code = exitcode.General
		default:
			code = exitcode.Topic
		}
		return zero, code, fmt.Errorf("recv: %w: %s", errRequestRejected, reply.Reason())
	}

	if data.Opcode() != expectedOpcode {
		return zero, exitcode.General, fmt.Errorf("recv: %w: %d", errUnexpectedOpcode, data.Opcode())
	}

	reply, ok := data.Payload().(P)
	if !ok {
		return zero, exitcode.General, fmt.Errorf("recv: %w", errUnexpectedPayload)
	}
	return reply, exitcode.Success, nil
}

// Command is a client command parsed from the command line, ready to run.
type Command interface {
	// Execute runs the command and returns the exit code of the client, which is
	// exitcode.Success when the error is nil. It returns once ctx is cancelled.
	Execute(ctx context.Context, env Env) (int, error)
}

// newFlagSet returns a flag set for the command name that reports errors
// instead of printing them or exiting.
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}
