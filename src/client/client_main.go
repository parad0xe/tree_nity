package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/protocol"
)

func main() {
	retcode := run(context.Background())
	os.Exit(retcode)
}

// run starts the client and returns the exit code of the process.
func run(ctx context.Context) (code int) {
	sigCtx, sigCancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	// TODO: parse "<ipc_identifier> <command> [arguments]" into runArgs and return exitcode.General on invalid arguments.
	args := runArgs{}

	app, err := newApplication(appConfig{
		serverIPCPath: "",
		// TODO: use protocol.Encode once the encoder is merged.
		encoder: func(data protocol.Data) ([]byte, error) {
			return make([]byte, 0), nil
		},
		decoder: protocol.NewDataFromFrame,
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

	if c, err := app.run(sigCtx, args); err != nil {
		if !errors.Is(err, context.Canceled) {
			logError("client:", err)
			return c
		}
	}

	return exitcode.Success
}

// logError writes a diagnostic line to stderr. A failed write is ignored
// because there is nowhere left to report it.
func logError(a ...any) {
	_, _ = fmt.Fprintln(os.Stderr, a...)
}
