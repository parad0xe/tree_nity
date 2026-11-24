package response

import (
	"context"
	"errors"
	"os"
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
// Other errors follow SendToClient's contract.
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
		sendErr := ctx.Err()
		if sendErr == nil {
			sendErr = sender.TimedSend(frame, time.Now())
			if errors.Is(sendErr, os.ErrDeadlineExceeded) {
				sendErr = nil
			}
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
