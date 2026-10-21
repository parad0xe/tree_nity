package protocol

import (
	"errors"
	"fmt"
)

var (
	// ErrMissingOpcode indicates that a frame has no operation code.
	ErrMissingOpcode = errors.New("missing opcode")
	// ErrMissingEmitter indicates that a frame has no sender type.
	ErrMissingEmitter = errors.New("missing emitter")
	// ErrMissingMetadata indicates that a frame has no metadata.
	ErrMissingMetadata = errors.New("missing metadata")
	// ErrMissingPayload indicates that a frame has no payload.
	ErrMissingPayload = errors.New("missing payload")
	// ErrUnknownOpcode indicates that an operation code is unsupported.
	ErrUnknownOpcode = errors.New("unknown opcode")
	// ErrUnknownEmitter indicates that a sender type is unsupported.
	ErrUnknownEmitter = errors.New("unknown emitter")
	// ErrMissingClientID indicates that client metadata has no client ID.
	ErrMissingClientID = errors.New("missing clientId")
	// ErrMissingClientIPC indicates that client metadata has no IPC endpoint.
	ErrMissingClientIPC = errors.New("missing clientIPC")
	// ErrMissingRaw indicates that a produce payload has no raw flag.
	ErrMissingRaw = errors.New("missing raw")
	// ErrMissingMessage indicates that a produce payload has no message.
	ErrMissingMessage = errors.New("missing message")
	// ErrMissingMessageKey indicates that a message has no key.
	ErrMissingMessageKey = errors.New("missing message key")
	// ErrMissingMessageValue indicates that a message has no value.
	ErrMissingMessageValue = errors.New("missing message value")
	// ErrMessageTooLarge indicates that a message exceeds MessageSize bytes.
	ErrMessageTooLarge = fmt.Errorf("message exceeds %d bytes", MessageSize)
	// ErrAdditionalJSONValue indicates that a frame contains another JSON value.
	ErrAdditionalJSONValue = errors.New("unexpected additional JSON value")
)
