package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/protocol"
)

// application holds the resources the server owns while it runs.
type application struct {
	inbound *ipc.Inbound
}

// newApplication creates the server inbound queue, named after the process
// ID so that several servers can run side by side.
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

// run receives frames until ctx is done or the inbound fails. Invalid frames
// are reported on stderr and skipped. It returns the error that stopped it,
// with the exit code that matches it.
func (app *application) run(ctx context.Context) (int, error) {
	if ctx.Err() != nil {
		return exitcode.Success, ctx.Err()
	}

	for {
		data, err := app.inbound.Recv(ctx)
		if ctx.Err() != nil {
			return exitcode.Success, ctx.Err()
		}
		if err != nil {
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
			logError("server: unhandled opcode")
		}
	}
}

// drop closes and removes the inbound queue, if it was created.
func (app *application) drop() error {
	var err error

	if app.inbound != nil {
		err = errors.Join(err, app.inbound.Drop())
	}

	return err
}
