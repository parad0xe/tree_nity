package ipc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

// sendSlice is the longest a send waits on a full queue before it checks its
// context again.
const sendSlice = 100 * time.Millisecond

// EncodeFunc turns protocol data into a frame.
type EncodeFunc func(protocol.Data) ([]byte, error)

// Outbound sends protocol data to a queue owned by another process.
type Outbound struct {
	ipcPath string
	sender  *mqueue.Sender
	encode  EncodeFunc
}

// NewOutbound opens the existing queue ipcPath to send protocol data encoded
// with encode. The returned error matches os.ErrNotExist when the queue does
// not exist.
func NewOutbound(ipcPath string, encode EncodeFunc) (*Outbound, error) {
	out := &Outbound{
		ipcPath: ipcPath,
		encode:  encode,
	}

	sender, err := mqueue.NewSender(out.ipcPath)
	if err != nil {
		return nil, err
	}
	out.sender = sender

	return out, nil
}

// Send waits while the queue is full, then sends data behind the frames
// already waiting. The returned error matches context.Canceled or
// context.DeadlineExceeded when ctx is done first, and ErrInvalidData when data
// cannot be encoded.
func (out *Outbound) Send(ctx context.Context, data protocol.Data) error {
	return out.send(ctx, data, out.sender.TimedSend)
}

// SendAhead waits while the queue is full, then sends data ahead of the frames
// already waiting. The returned error matches context.Canceled or
// context.DeadlineExceeded when ctx is done first, and ErrInvalidData when data
// cannot be encoded.
func (out *Outbound) SendAhead(ctx context.Context, data protocol.Data) error {
	return out.send(ctx, data, out.sender.TimedSendAhead)
}

// send encodes data and hands the frame to sendFrame, one deadline of at most
// sendSlice at a time, until the frame is sent or ctx is done.
func (out *Outbound) send(ctx context.Context, data protocol.Data, sendFrame func([]byte, time.Time) error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	frame, err := out.encode(data)
	if err != nil {
		return fmt.Errorf("send to outbound %q: %w: %w", out.ipcPath, ErrInvalidData, err)
	}

	for {
		deadline := time.Now().Add(sendSlice)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}

		err := sendFrame(frame, deadline)
		if err == nil {
			return nil
		}

		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return fmt.Errorf("send to outbound %q: %w", out.ipcPath, err)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// IPCPath returns the name of the queue this outbound sends to.
func (out *Outbound) IPCPath() string {
	return out.ipcPath
}

// Close closes the connection. The queue stays available to its owner.
func (out *Outbound) Close() error {
	return out.sender.Close()
}
