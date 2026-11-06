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

// defaultGracePeriod is the time the command has to finish after SIGINT or
// SIGTERM.
const defaultGracePeriod = 4 * time.Second

// defaultCancelTimeout is the time the command has to stop once cancelled.
const defaultCancelTimeout = 1 * time.Second

// commandParsers maps each command name to the function that parses its arguments.
var commandParsers = map[string]func(args []string) (commands.Command, error){
	"produce":   commands.ParseProduce,
	"info":      commands.ParseInfo,
	"subscribe": commands.ParseSubscribe,
	"list":      commands.ParseList,
	// TODO: add the create command.
}

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
		encoder:       protocol.Encode,
		decoder:       protocol.Decode,
		gracePeriod:   defaultGracePeriod,
		cancelTimeout: defaultCancelTimeout,
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
	serverIPCPath, commandName := args[0], args[1]

	parseCommand, ok := commandParsers[commandName]
	if !ok {
		return "", nil, fmt.Errorf("%w: %s", errUnknownCommand, commandName)
	}
	command, err := parseCommand(args[2:])
	if err != nil {
		return "", nil, err
	}

	return serverIPCPath, command, nil
}

// logError writes a diagnostic line to stderr. A failed write is ignored
// because there is nowhere left to report it.
func logError(a ...any) {
	_, _ = fmt.Fprintln(os.Stderr, a...)
}
