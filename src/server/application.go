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
	"tree_nity/src/server/response"
	"tree_nity/src/server/session/topic"
)

const (
	// gracePeriod is the longest the server keeps running after SIGINT or SIGTERM.
	gracePeriod = 5 * time.Second
	// shutdownPollInterval is how often listen checks for a shutdown request.
	shutdownPollInterval = 200 * time.Millisecond
	// replyTimeout is the longest a response waits while the client queue is full.
	replyTimeout = 2 * time.Second
)

// application holds the resources the server owns while it runs.
type application struct {
	inbound      *ipc.Inbound
	topicManager *topic.Manager
}

// newApplication initializes topic state and creates the server queue, named after the process ID.
// The returned error matches os.ErrExist when the queue already exists.
func newApplication() (*application, error) {
	pid := os.Getpid()
	ipcPath := fmt.Sprintf("/operator.server.%d", pid)

	app := &application{topicManager: topic.NewManager()}

	inbound, err := ipc.NewInbound(ipcPath, protocol.Decode)
	if err != nil {
		return nil, err
	}
	app.inbound = inbound

	return app, nil
}

// run serves requests until shutdown is requested, then for at most gracePeriod.
// Canceling ctx interrupts IPC; receive failures return exitcode.IPC.
func (app *application) run(ctx context.Context, shutdown lifecycle.GracefulShutdown) (int, error) {
	if ctx.Err() != nil {
		return exitcode.Success, ctx.Err()
	}

	done := make(chan lifecycle.ExitResult, 1)

	// A shutdown request leaves runCtx active during the grace period.
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	go func() {
		code, err := app.listen(runCtx, shutdown)
		done <- lifecycle.NewExitResult(code, err)
	}()

	select {
	case res := <-done:
		return res.Code(), res.Err()
	case <-shutdown.Requested():
	}

	select {
	case res := <-done:
		return res.Code(), res.Err()
	case <-time.After(gracePeriod):
	}

	// The inbound must not be dropped while Recv is running.
	runCancel()
	res := <-done
	if errors.Is(res.Err(), context.Canceled) && ctx.Err() == nil {
		return exitcode.Success, nil
	}

	return res.Code(), res.Err()
}

// listen receives requests until the server can stop.
// Invalid frames and recoverable handler errors are logged and skipped.
// Receive failures return exitcode.IPC; canceled handlers retain their dispatch code.
func (app *application) listen(ctx context.Context, shutdown lifecycle.GracefulShutdown) (int, error) {
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

		if code, err := app.dispatch(ctx, data); err != nil {
			if ctx.Err() != nil {
				return code, err
			}
			logError("server:", err)
		}
	}
}

// dispatch routes validated client requests and returns their exit status.
func (app *application) dispatch(ctx context.Context, data protocol.Data) (int, error) {
	metadata, ok := data.Metadata().(*protocol.ClientMetadata)
	if !ok {
		return exitcode.General, errUnhandledOpcode
	}

	sendCtx, sendCancel := context.WithTimeout(ctx, replyTimeout)
	defer sendCancel()

	var err error
	// Decoding guarantees that the payload type matches the opcode.
	switch payload := data.Payload().(type) {
	// TODO: enable request routes when their handlers can send a response.
	// case *protocol.ListTopicPayload:
	// 	app.topicManager.ListTopics(*metadata, *payload)
	case *protocol.CreateTopicPayload:
		err = app.topicManager.CreateTopic(sendCtx, ctx, *metadata, *payload)
	// case *protocol.TopicSubscriptionPayload:
	// 	app.topicManager.NewSubscription(*metadata, *payload)
	// case *protocol.CheckTopicPayload:
	// 	app.topicManager.CheckTopic(*metadata, *payload)
	// case *protocol.ProduceMessagePayload:
	// 	app.topicManager.ProduceMessage(*metadata, *payload)
	case *protocol.AcknowledgementPayload, *protocol.ConsumerHeartbeatPayload,
		*protocol.ConsumerDisconnectPayload:
		// TODO: route notifications to consumer handlers once they are implemented.
		return exitcode.General, errUnhandledOpcode
	default:
		err = response.SendToClient(
			sendCtx,
			*metadata,
			protocol.ErrorReply,
			protocol.NewErrorReplyPayload(protocol.InvalidRequest, errUnhandledOpcode.Error()),
		)
		if err != nil {
			err = errors.Join(errUnhandledOpcode, err)
		}
	}
	if err != nil {
		if errors.Is(err, response.ErrReply) && !errors.Is(err, ipc.ErrInvalidData) {
			return exitcode.IPC, err
		}
		return exitcode.General, err
	}
	return exitcode.Success, nil
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
