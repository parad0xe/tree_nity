package response

import (
	"context"
	"errors"
	"time"

	"tree_nity/src/ipc"
	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

// SendToClient sends a server response to the client's IPC queue and closes the outbound handle.
// Validation and queue-opening errors are returned directly; communication failures match ErrReply.
func SendToClient(ctx context.Context, metadata protocol.ClientMetadata, opcode protocol.Opcode, payload protocol.Payload) error {
	return send(opcode, payload, func(data protocol.Data) error {
		outbound, err := ipc.NewOutbound(metadata.ClientIPC(), protocol.Encode)
		if err != nil {
			return err
		}
		sendErr := outbound.Send(ctx, data)
		closeErr := outbound.Close()
		if err := errors.Join(sendErr, closeErr); err != nil {
			return errors.Join(ErrReply, err)
		}
		return nil
	})
}

// TrySendToClient attempts one immediate send, dropping the response if the queue is full.
// A full queue matches os.ErrDeadlineExceeded and ErrReply; other errors follow SendToClient.
func TrySendToClient(ctx context.Context, metadata protocol.ClientMetadata, opcode protocol.Opcode, payload protocol.Payload) error {
	return send(opcode, payload, func(data protocol.Data) error {
		frame, err := protocol.Encode(data)
		if err != nil {
			return err
		}
		sender, err := mqueue.NewSender(metadata.ClientIPC())
		if err != nil {
			return err
		}
		deadline := time.Now()
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
		sendErr := ctx.Err()
		if sendErr == nil {
			sendErr = sender.TimedSend(frame, deadline)
		}
		closeErr := sender.Close()
		if err := errors.Join(sendErr, closeErr); err != nil {
			return errors.Join(ErrReply, err)
		}
		return nil
	})
}

func send(opcode protocol.Opcode, payload protocol.Payload, sendData func(protocol.Data) error) error {
	data, err := protocol.NewData(
		opcode,
		protocol.NewServerMetadata(),
		payload,
	)
	if err != nil {
		return err
	}
	return sendData(data)
}
