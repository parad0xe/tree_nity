package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"tree_nity/src/exitcode"
)

func main() {
	retcode := run(context.Background(), os.Stdout)
	os.Exit(retcode)
}

// run starts the server and prints its IPC identifier on stdout, then receives
// frames until SIGINT, SIGTERM or the end of ctx. It returns exitcode.General
// when the server cannot start or clean up, and exitcode.IPC when receiving
// fails.
func run(ctx context.Context, stdout io.Writer) (code int) {
	sigCtx, sigCancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer sigCancel()

	app, err := newApplication()
	if err != nil {
		logError("server:", err)
		// A startup failure is a general error; IPC covers failures while communicating.
		return exitcode.General
	}
	defer func() {
		if err := app.drop(); err != nil {
			logError("server:", err)
			code = exitcode.General
		}
	}()

	if _, err := fmt.Fprintln(stdout, app.inbound.IPCPath()); err != nil {
		logError("server:", fmt.Errorf("%w: %w", errPrintIPCPath, err))
		return exitcode.General
	}

	if c, err := app.run(sigCtx); err != nil {
		if !errors.Is(err, context.Canceled) {
			logError("server:", err)
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
