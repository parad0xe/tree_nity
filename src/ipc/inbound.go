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

// DecodeFunc turns a frame into protocol data.
type DecodeFunc func([]byte) (protocol.Data, error)

// Inbound owns the queue through which a process receives protocol data.
type Inbound struct {
	ipcPath  string
	receiver *mqueue.Receiver
	decode   DecodeFunc
}

// NewInbound creates the queue ipcPath and opens it to receive protocol data
// decoded with decode. The returned error matches ErrNilDecoder when decode is
// nil, mqueue.ErrInvalidName when ipcPath is not a valid queue name, and
// os.ErrExist when the queue already exists.
func NewInbound(ipcPath string, decode DecodeFunc) (*Inbound, error) {
	if decode == nil {
		return nil, ErrNilDecoder
	}

	in := &Inbound{
		ipcPath: ipcPath,
		decode:  decode,
	}

	receiver, err := mqueue.NewReceiver(in.ipcPath)
	if err != nil {
		return nil, err
	}
	in.receiver = receiver

	return in, nil
}

// Recv waits until a frame arrives, then returns the decoded protocol data. The
// returned error matches context.Canceled or context.DeadlineExceeded when ctx
// is done first, and ErrInvalidFrame when the frame is not valid protocol data.
// Only one goroutine may call Recv at a time.
func (in *Inbound) Recv(ctx context.Context) (protocol.Data, error) {
	return in.recv(ctx, in.receiver.TimedRecv)
}

// recv waits for a frame with recvFrame, one deadline of at most waitSlice at a
// time, until a frame arrives or ctx is done.
func (in *Inbound) recv(ctx context.Context, recvFrame func(time.Time) ([]byte, error)) (protocol.Data, error) {
	if ctx.Err() != nil {
		return protocol.Data{}, ctx.Err()
	}

	for {
		deadline := sliceDeadline(ctx)

		frame, err := recvFrame(deadline)
		if err == nil {
			return in.decodeFrame(frame)
		}

		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return protocol.Data{}, fmt.Errorf("receive from inbound %q: %w", in.ipcPath, err)
		}

		if ctx.Err() != nil {
			return protocol.Data{}, ctx.Err()
		}
	}
}

// decodeFrame turns frame into protocol data with the decoder of the inbound.
func (in *Inbound) decodeFrame(frame []byte) (protocol.Data, error) {
	data, err := in.decode(frame)
	if err != nil {
		return protocol.Data{}, fmt.Errorf("receive from inbound %q: %w: %w", in.ipcPath, ErrInvalidFrame, err)
	}

	return data, nil
}

// IPCPath returns the name of the queue, which other processes use to reach it.
func (in *Inbound) IPCPath() string {
	return in.ipcPath
}

// Unlink removes the queue from the system without closing it. It may be called
// while Recv is running.
func (in *Inbound) Unlink() error {
	return in.receiver.Unlink()
}

// Drop closes the queue and removes it from the system. It must not be called
// while Recv is running.
func (in *Inbound) Drop() error {
	return in.receiver.Drop()
}
