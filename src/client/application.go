package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"tree_nity/src/client/commands"
	"tree_nity/src/ipc"
	"tree_nity/src/lifecycle"
)

// application holds the resources the client owns while it runs.
type application struct {
	// inbound receives what the server sends to the client.
	inbound *ipc.Inbound
	// outbound sends the requests of the client to the server.
	outbound *ipc.Outbound
	// gracePeriod is the longest the client keeps running after SIGINT or SIGTERM.
	gracePeriod time.Duration
}

// appConfig holds what newApplication needs to start the client.
type appConfig struct {
	// serverIPCPath is the IPC identifier printed by the server.
	serverIPCPath string
	// encoder turns the data sent to the server into frames.
	encoder ipc.EncodeFunc
	// decoder turns the frames received from the server into data.
	decoder ipc.DecodeFunc
	// gracePeriod is the longest the client keeps running after SIGINT or SIGTERM.
	gracePeriod time.Duration
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
		gracePeriod: config.gracePeriod,
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
// most gracePeriod to finish.
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

	runCancel()
	res := <-done

	return res.Code(), res.Err()
}

// drop removes the client queue and closes the server queue, if they were
// opened.
func (app *application) drop() error {
	var err error

	if app.inbound != nil {
		err = errors.Join(err, app.inbound.Drop())
	}

	if app.outbound != nil {
		err = errors.Join(err, app.outbound.Close())
	}

	return err
}
