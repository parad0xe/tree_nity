package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
)

const (
	// gracePeriod is the longest the server keeps running after SIGINT or SIGTERM.
	gracePeriod = 5 * time.Second
	// shutdownPollInterval is how often listen checks for a shutdown request.
	shutdownPollInterval = 200 * time.Millisecond
)

// exitResult is an exit code with the error that caused it.
type exitResult struct {
	code int
	err  error
}

// application holds the resources the server owns while it runs.
type application struct {
	inbound *ipc.Inbound
}

// newApplication creates the server inbound queue, named after the process ID.
// The returned error matches os.ErrExist when the queue already exists.
func newApplication() (*application, error) {
	pid := os.Getpid()
	ipcPath := fmt.Sprintf("/operator.server.%d", pid)

	app := &application{}

	inbound, err := ipc.NewInbound(ipcPath, protocol.NewDataFromFrame)
	if err != nil {
		return nil, err
	}
	app.inbound = inbound

	return app, nil
}

// run listens for frames until sigCtx is done, then for at most gracePeriod.
// The returned error matches context.Canceled when sigCtx is done before run
// starts, and is the error of Recv, with exitcode.IPC, when receiving fails.
func (app *application) run(sigCtx context.Context) (int, error) {
	if sigCtx.Err() != nil {
		return exitcode.Success, sigCtx.Err()
	}

	shutdown := lifecycle.NewGracefulShutdown(sigCtx.Done())
	done := make(chan exitResult, 1)

	// runCtx does not derive from sigCtx: a signal must not stop listen.
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	go func() {
		code, err := app.listen(runCtx, shutdown)
		done <- exitResult{code, err}
	}()

	select {
	case res := <-done:
		return res.code, res.err
	case <-sigCtx.Done():
	}

	select {
	case res := <-done:
		return res.code, res.err
	case <-time.After(gracePeriod):
	}

	// The inbound must not be dropped while Recv is running.
	runCancel()
	if res := <-done; !errors.Is(res.err, context.Canceled) {
		return res.code, res.err
	}

	return exitcode.Success, nil
}

// listen receives frames until the server is asked to stop and is ready to.
// Invalid frames are logged and skipped. The returned error matches
// context.Canceled or context.DeadlineExceeded when ctx is done first, and is
// the error of Recv, with exitcode.IPC, when receiving fails.
func (app *application) listen(ctx context.Context, shutdown lifecycle.GracefulShutdown) (int, error) {
	for {
		if shutdown.Requested() && app.readyToFinish() {
			return exitcode.Success, nil
		}

		recvCtx, recvCancel := context.WithTimeout(ctx, shutdownPollInterval)
		data, err := app.inbound.Recv(recvCtx)
		recvCancel()

		if ctx.Err() != nil {
			return exitcode.Success, ctx.Err()
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if errors.Is(err, ipc.ErrInvalidFrame) {
				logError("server:", err)
				continue
			}

			return exitcode.IPC, err
		}

		switch data {
		// TODO: dispatch each opcode to its handler
		default:
			// TODO: reply to the client with an error once the server answers requests
			logError("server:", errUnhandledOpcode)
		}
	}
}

// readyToFinish reports whether the server can stop cleanly.
func (app *application) readyToFinish() bool {
	// TODO: return true only once every consumer has committed or disconnected and no topic goroutine is still running.
	return true
}

// drop closes and removes the inbound queue, if it was created. The returned
// error joins the close and unlink errors.
func (app *application) drop() error {
	var err error

	if app.inbound != nil {
		err = errors.Join(err, app.inbound.Drop())
	}

	return err
}
