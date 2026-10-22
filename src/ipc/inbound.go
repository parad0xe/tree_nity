package ipc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

// wakeFrame unblocks a pending Recv. A single null byte is never valid
// protocol data, so it cannot be mistaken for a frame sent by a peer.
var wakeFrame = []byte{0}

// DecodeFunc turns a frame into protocol data.
type DecodeFunc func([]byte) (protocol.Data, error)

// Inbound owns the queue through which a process receives protocol data.
type Inbound struct {
	ipcPath  string
	receiver *mqueue.Receiver
	loopback *mqueue.Sender
	decode   DecodeFunc
}

// NewInbound creates the queue /<prefix>.<pid> and opens it to receive
// protocol data decoded with decode. The returned error matches os.ErrExist
// when the queue already exists.
func NewInbound(prefix string, decode DecodeFunc) (*Inbound, error) {
	prefix = strings.TrimPrefix(prefix, "/")
	prefix = strings.TrimSuffix(prefix, ".")

	pid := os.Getpid()

	in := &Inbound{
		ipcPath: fmt.Sprintf("/%s.%d", prefix, pid),
		decode:  decode,
	}

	receiver, err := mqueue.NewReceiver(in.ipcPath)
	if err != nil {
		return nil, err
	}
	in.receiver = receiver

	loopback, err := mqueue.NewSender(in.ipcPath)
	if err != nil {
		return nil, errors.Join(err, receiver.Drop())
	}
	in.loopback = loopback

	return in, nil
}

// Recv blocks until a frame arrives or ctx is done, and returns the decoded
// protocol data. The returned error matches context.Canceled or
// context.DeadlineExceeded when ctx is done, and ErrInvalidFrame when the frame
// is not valid protocol data. Only one goroutine may call Recv at a time.
func (in *Inbound) Recv(ctx context.Context) (protocol.Data, error) {
	if ctx.Err() != nil {
		return protocol.Data{}, ctx.Err()
	}

	stop := context.AfterFunc(ctx, func() {
		// The error cannot be reported from here. Drop must not run while Recv
		// does, so the loopback is still open and the wake-up can be sent.
		_ = in.loopback.SendAhead(wakeFrame)
	})
	defer stop()

	for {
		frame, err := in.receiver.Recv()
		if err != nil {
			return protocol.Data{}, fmt.Errorf("receive from inbound %q: %w", in.ipcPath, err)
		}

		if bytes.Equal(frame, wakeFrame) {
			if ctx.Err() != nil {
				return protocol.Data{}, ctx.Err()
			}

			continue
		}

		data, err := in.decode(frame)
		if err != nil {
			return protocol.Data{}, fmt.Errorf("receive from inbound %q: %w: %w", in.ipcPath, ErrInvalidFrame, err)
		}

		return data, nil
	}
}

// IPCPath returns the name of the queue, which other processes use to reach it.
func (in *Inbound) IPCPath() string {
	return in.ipcPath
}

// Drop closes the queue and removes it from the system. It must not be called
// while Recv is running.
func (in *Inbound) Drop() error {
	var err error

	if in.receiver != nil {
		err = errors.Join(err, in.receiver.Drop())
	}

	if in.loopback != nil {
		err = errors.Join(err, in.loopback.Close())
	}

	return err
}
