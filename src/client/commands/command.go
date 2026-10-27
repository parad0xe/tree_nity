package commands

import (
	"context"

	"tree_nity/src/ipc"
	"tree_nity/src/lifecycle"
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

// Command is a client command parsed from the command line, ready to run.
type Command interface {
	// Execute runs the command and returns the exit code of the client.
	Execute(context.Context, Env) (int, error)
}
