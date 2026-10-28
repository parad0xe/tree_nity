package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tree_nity/src/client/commands"
	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/protocol"
)

// usage describes the command line of the client.
const usage = "usage: %s <ipc_identifier> <command> [arguments]"

// defaultGracePeriod is the longest the client keeps running after SIGINT or
// SIGTERM.
const defaultGracePeriod = 5 * time.Second

func main() {
	retcode := run(context.Background())
	os.Exit(retcode)
}

// run starts the client and returns the exit code of the process.
func run(ctx context.Context) (code int) {
	sigCtx, sigCancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	serverIPCPath, command, err := parse(os.Args[1:])
	if err != nil {
		logError("client:", err)
		logError(fmt.Errorf(usage, os.Args[0]))
		return exitcode.General
	}

	app, err := newApplication(appConfig{
		serverIPCPath: serverIPCPath,
		// TODO: use protocol.Encode once the encoder is merged.
		encoder: func(data protocol.Data) ([]byte, error) {
			return make([]byte, 0), nil
		},
		decoder:     protocol.NewDataFromFrame,
		gracePeriod: defaultGracePeriod,
	})
	if err != nil {
		logError("client:", err)
		// A startup failure is a general error; IPC covers failures while communicating.
		return exitcode.General
	}
	defer func() {
		if err := app.drop(); err != nil {
			logError("client:", err)
			code = exitcode.General
		}
	}()

	if c, err := app.run(sigCtx, command); err != nil {
		if !errors.Is(err, context.Canceled) {
			logError("client:", err)
			return c
		}
	}

	return exitcode.Success
}

// parse returns the server IPC identifier and the command requested by args.
func parse(args []string) (string, commands.Command, error) {
	if len(args) < 1 {
		return "", nil, errMissingIPCIdentifier
	}

	if len(args) < 2 {
		return "", nil, errMissingCommand
	}
	commandName := args[1]

	switch commandName {
	// TODO: parse the produce, create, list, subscribe and info commands.
	default:
		return "", nil, fmt.Errorf("%w: %s", errUnknownCommand, commandName)
	}
}

// logError writes a diagnostic line to stderr. A failed write is ignored
// because there is nowhere left to report it.
func logError(a ...any) {
	_, _ = fmt.Fprintln(os.Stderr, a...)
}
