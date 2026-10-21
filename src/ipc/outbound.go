package ipc

import (
	"fmt"

	"tree_nity/src/ipc/mqueue"
	"tree_nity/src/ipc/protocol"
)

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

// Send blocks while the queue is full, then sends data behind the frames
// already waiting. The returned error matches ErrInvalidData when data cannot
// be encoded.
func (out *Outbound) Send(data protocol.Data) error {
	return out.send(data, out.sender.Send)
}

// SendAhead blocks while the queue is full, then sends data ahead of the frames
// already waiting. The returned error matches ErrInvalidData when data cannot
// be encoded.
func (out *Outbound) SendAhead(data protocol.Data) error {
	return out.send(data, out.sender.SendAhead)
}

// send encodes data and hands the frame to sendFrame.
func (out *Outbound) send(data protocol.Data, sendFrame func([]byte) error) error {
	frame, err := out.encode(data)
	if err != nil {
		return fmt.Errorf("send to outbound %q: %w: %w", out.ipcPath, ErrInvalidData, err)
	}

	if err := sendFrame(frame); err != nil {
		return fmt.Errorf("send to outbound %q: %w", out.ipcPath, err)
	}

	return nil
}

// IPCPath returns the name of the queue this outbound sends to.
func (out *Outbound) IPCPath() string {
	return out.ipcPath
}

// Close closes the connection. The queue stays available to its owner.
func (out *Outbound) Close() error {
	return out.sender.Close()
}
