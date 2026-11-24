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
	// DefaultRecvTimeout is the longest a command waits for the server reply.
	DefaultRecvTimeout = 2 * time.Second
	// DefaultSendTimeout is the longest a request waits while the server queue is full.
	DefaultSendTimeout = 2 * time.Second
	// DefaultHeartbeatSendTimeout is the longest a heartbeat waits while the server queue is full.
	DefaultHeartbeatSendTimeout = 100 * time.Millisecond
)

// Env holds what every command can use while it runs.
type Env struct {
	// Shutdown is requested after SIGINT or SIGTERM.
	Shutdown lifecycle.GracefulShutdown
	// Inbound receives what the server sends to the client.
	Inbound *ipc.Inbound
	// Outbound sends the requests of the client to the server.
	Outbound *ipc.Outbound
	// Stdout receives the non-error output of the command.
	Stdout io.Writer
	// Stdin provides the input of the command.
	Stdin io.ReadCloser
	// RecvTimeout is the longest a command waits for the server reply.
	RecvTimeout time.Duration
	// SendTimeout is the longest a request waits while the server queue is full.
	SendTimeout time.Duration
	// HeartbeatInterval is the time between two heartbeats of a subscriber.
	HeartbeatInterval time.Duration
	// HeartbeatSendTimeout is the longest a heartbeat waits while the server queue is full.
	HeartbeatSendTimeout time.Duration
}

// createData returns a client request.
func (env *Env) createData(opcode protocol.Opcode, payload protocol.Payload) (protocol.Data, error) {
	return protocol.NewData(opcode, protocol.NewClientMetadata(env.Inbound.IPCPath()), payload)
}

// send sends data to the server.
func (env *Env) send(ctx context.Context, data protocol.Data) (int, error) {
	sendCtx, sendCancel := context.WithTimeout(ctx, env.SendTimeout)
	defer sendCancel()

	if err := env.Outbound.Send(sendCtx, data); err != nil {
		return exitcode.IPC, fmt.Errorf("send: %w", err)
	}

	return exitcode.Success, nil
}

// recv returns the payload of the server reply with expectedOpcode.
func (env *Env) recv[P protocol.Payload](ctx context.Context, expectedOpcode protocol.Opcode) (P, int, error) {
	var zero P

	recvCtx, recvCancel := context.WithTimeout(ctx, env.RecvTimeout)
	defer recvCancel()

	data, code, err := env.recvData(recvCtx)
	if err != nil {
		return zero, code, err
	}

	if code, err := replyError(data); err != nil {
		return zero, code, err
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

// recvData returns the next data from the server.
func (env *Env) recvData(ctx context.Context) (protocol.Data, int, error) {
	data, err := env.Inbound.Recv(ctx)
	if err != nil {
		return protocol.Data{}, exitcode.IPC, fmt.Errorf("recv: %w", err)
	}

	if data.Emitter() != protocol.Server {
		return protocol.Data{}, exitcode.General, fmt.Errorf("recv: %w", errUnexpectedEmitter)
	}

	return data, exitcode.Success, nil
}

// replyError returns the error of a server reply refusing the request.
func replyError(data protocol.Data) (int, error) {
	if data.Opcode() == protocol.ErrorReply {
		reply, ok := data.Payload().(*protocol.ErrorReplyPayload)
		if !ok {
			return exitcode.General, fmt.Errorf("recv: %w", errUnexpectedPayload)
		}

		var code int
		switch reply.Kind() {
		case protocol.InvalidRequest:
			code = exitcode.General
		default:
			code = exitcode.Topic
		}
		return code, fmt.Errorf("recv: %w: %s", errRequestRejected, reply.Reason())
	}

	if data.Opcode() == protocol.ServerShutdown {
		return exitcode.IPC, fmt.Errorf("recv: %w", errServerShuttingDown)
	}

	return exitcode.Success, nil
}

// print writes raw bytes to the command output.
func (env *Env) print(b []byte) error {
	_, err := env.Stdout.Write(b)
	return err
}

// println writes a line of text to the command output.
func (env *Env) println(s string) error {
	_, err := io.WriteString(env.Stdout, s+"\n")
	return err
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
