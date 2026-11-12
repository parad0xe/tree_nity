package ipc

import "errors"

var (
	// ErrInvalidFrame reports a received frame that is not valid protocol data.
	ErrInvalidFrame = errors.New("invalid frame")
	// ErrInvalidData reports protocol data that cannot be encoded into a frame.
	ErrInvalidData = errors.New("invalid data")
	// ErrNilDecoder reports an inbound requested without a decoder.
	ErrNilDecoder = errors.New("nil decoder")
	// ErrNilEncoder reports an outbound requested without an encoder.
	ErrNilEncoder = errors.New("nil encoder")
)
