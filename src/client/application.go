package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"tree_nity/src/client/commands"
	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/lifecycle"
)

// application holds the resources the client owns while it runs.
type application struct {
	// inbound receives what the server sends to the client.
	inbound *ipc.Inbound
	// outbound sends the requests of the client to the server.
	outbound *ipc.Outbound
	// gracePeriod is the time the command has to finish after SIGINT or SIGTERM.
	gracePeriod time.Duration
	// cancelTimeout is the time the command has to stop once cancelled.
	cancelTimeout time.Duration
	// gracefulShutdownFailed reports that the command did not stop once
	// cancelled and may still use the queues.
	gracefulShutdownFailed bool
}

// appConfig holds what newApplication needs to start the client.
type appConfig struct {
	// serverIPCPath is the IPC identifier printed by the server.
	serverIPCPath string
	// encoder turns the data sent to the server into frames.
	encoder ipc.EncodeFunc
	// decoder turns the frames received from the server into data.
	decoder ipc.DecodeFunc
	// gracePeriod is the time the command has to finish after SIGINT or SIGTERM.
	gracePeriod time.Duration
	// cancelTimeout is the time the command has to stop once cancelled.
	cancelTimeout time.Duration
}

// clientIPCPath returns the inbound queue name of the client running as pid.
func clientIPCPath(pid int) string {
	return fmt.Sprintf("/operator.client.%d", pid)
}

// newApplication connects to the server, then creates the client inbound queue,
// named after the process ID.
func newApplication(config appConfig) (*application, error) {
	ipcPath := clientIPCPath(os.Getpid())

	app := &application{
		gracePeriod:   config.gracePeriod,
		cancelTimeout: config.cancelTimeout,
	}

	outbound, err := ipc.NewOutbound(config.serverIPCPath, config.encoder)
	if err != nil {
		return nil, err
	}
	app.outbound = outbound

	inbound, err := ipc.NewInbound(ipcPath, config.decoder)
	if err != nil {
		return nil, errors.Join(err, outbound.Close())
	}
	app.inbound = inbound

	return app, nil
}

// run runs the requested command. After SIGINT or SIGTERM, the command has at
// most gracePeriod to finish, then at most cancelTimeout to stop. If it does
// not stop, run returns errCommandNotStopped.
func (app *application) run(sigCtx context.Context, command commands.Command) (int, error) {
	env := commands.Env{
		Shutdown: lifecycle.NewGracefulShutdown(sigCtx.Done()),
		Inbound:  app.inbound,
		Outbound: app.outbound,
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	done := make(chan lifecycle.ExitResult, 1)
	go func() {
		c, err := command.Execute(runCtx, env)
		done <- lifecycle.NewExitResult(c, err)
	}()

	select {
	case res := <-done:
		return res.Code(), res.Err()
	case <-sigCtx.Done():
	}

	select {
	case res := <-done:
		return res.Code(), res.Err()
	case <-time.After(app.gracePeriod):
	}

	// A command that does not stop once cancelled is left running.
	runCancel()
	select {
	case res := <-done:
		return res.Code(), res.Err()
	case <-time.After(app.cancelTimeout):
		app.gracefulShutdownFailed = true
		return exitcode.General, errCommandNotStopped
	}
}

// drop removes the client queue and closes the server queue, if they were
// opened. After a failed graceful shutdown, it only removes the client queue
// name and leaves both queues open.
func (app *application) drop() error {
	var err error

	if app.inbound != nil {
		if app.gracefulShutdownFailed {
			err = errors.Join(err, app.inbound.Unlink())
		} else {
			err = errors.Join(err, app.inbound.Drop())
		}
	}

	if app.outbound != nil && !app.gracefulShutdownFailed {
		err = errors.Join(err, app.outbound.Close())
	}

	return err
}
