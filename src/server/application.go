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
	"tree_nity/src/server/session/topic"
)

const (
	// gracePeriod is the longest the server keeps running after SIGINT or SIGTERM.
	gracePeriod = 5 * time.Second
	// shutdownPollInterval is how often listen checks for a shutdown request.
	shutdownPollInterval = 200 * time.Millisecond
)

// application holds the resources the server owns while it runs.
type application struct {
	inbound      *ipc.Inbound
	topicManager *topic.Manager
}

// newApplication creates the server inbound queue, named after the process ID.
// The returned error matches os.ErrExist when the queue already exists.
func newApplication() (*application, error) {
	pid := os.Getpid()
	ipcPath := fmt.Sprintf("/operator.server.%d", pid)

	app := &application{}

	inbound, err := ipc.NewInbound(ipcPath, protocol.Decode)
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
	done := make(chan lifecycle.ExitResult, 1)

	// runCtx does not derive from sigCtx: a signal must not stop listen.
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	go func() {
		code, err := app.listen(runCtx, shutdown)
		done <- lifecycle.NewExitResult(code, err)
	}()

	select {
	case res := <-done:
		return res.Code(), res.Err()
	case <-sigCtx.Done():
	}

	select {
	case res := <-done:
		return res.Code(), res.Err()
	case <-time.After(gracePeriod):
	}

	// The inbound must not be dropped while Recv is running.
	runCancel()
	if res := <-done; !errors.Is(res.Err(), context.Canceled) {
		return res.Code(), res.Err()
	}

	return exitcode.Success, nil
}

// listen initializes topic state and receives requests until the server can stop.
// Invalid frames and handler errors are logged and skipped. The returned error matches
// context.Canceled or context.DeadlineExceeded when ctx is done first, and is
// the error of Recv, with exitcode.IPC, when receiving fails.
func (app *application) listen(ctx context.Context, shutdown lifecycle.GracefulShutdown) (int, error) {
	// The manager shares the listener's lifetime so shutdown also cancels IPC replies.
	app.topicManager = topic.NewManager(ctx)

	for {
		if shutdown.IsRequested() && app.readyToFinish() {
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

		if err := app.dispatch(ctx, data); err != nil {
			if ctx.Err() != nil {
				return exitcode.Success, ctx.Err()
			}
			logError("server:", err)
		}
	}
}

// dispatch routes validated client requests to the topic manager.
func (app *application) dispatch(ctx context.Context, data protocol.Data) error {
	metadata, ok := data.Metadata().(*protocol.ClientMetadata)
	if !ok {
		return errUnhandledOpcode
	}

	// Decoding guarantees that the payload type matches the opcode.
	switch payload := data.Payload().(type) {
	case *protocol.ListTopicPayload:
		app.topicManager.ListTopics(*metadata, *payload)
	case *protocol.CreateTopicPayload:
		return app.topicManager.CreateTopic(*metadata, *payload)
	case *protocol.TopicSubscriptionPayload:
		app.topicManager.NewSubscription(*metadata, *payload)
	case *protocol.CheckTopicPayload:
		app.topicManager.CheckTopic(*metadata, *payload)
	case *protocol.ProduceMessagePayload:
		app.topicManager.ProduceMessage(*metadata, *payload)
	default:
		reply, err := protocol.NewData(
			protocol.ErrorReply,
			protocol.NewServerMetadata(),
			protocol.NewErrorReplyPayload(protocol.InvalidRequest, errUnhandledOpcode.Error()),
		)
		if err != nil {
			return errors.Join(errUnhandledOpcode, err)
		}
		outbound, err := ipc.NewOutbound(metadata.ClientIPC(), protocol.Encode)
		if err != nil {
			return errors.Join(errUnhandledOpcode, err)
		}
		sendErr := outbound.Send(ctx, reply)
		return errors.Join(errUnhandledOpcode, sendErr, outbound.Close())
	}
	return nil
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
