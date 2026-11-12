package ipc

import (
	"context"
	"errors"

	"tree_nity/src/ipc/protocol"
)

// Reply sends a server response to the client's IPC queue and closes the outbound handle.
// Validation and queue-opening errors are returned directly; communication failures match ErrReply.
func Reply(ctx context.Context, metadata protocol.ClientMetadata, opcode protocol.Opcode, payload protocol.Payload) error {
	data, err := protocol.NewData(
		opcode,
		protocol.NewServerMetadata(),
		payload,
	)
	if err != nil {
		return err
	}

	outbound, err := NewOutbound(metadata.ClientIPC(), protocol.Encode)
	if err != nil {
		return err
	}

	sendErr := outbound.Send(ctx, data)
	closeErr := outbound.Close()
	if err := errors.Join(sendErr, closeErr); err != nil {
		return errors.Join(ErrReply, err)
	}

	return nil
}
