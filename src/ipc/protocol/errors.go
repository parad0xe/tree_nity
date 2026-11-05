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
	// ErrMissingTopic indicates a missing topic field.
	ErrMissingTopic = errors.New("missing topic")
	// ErrMissingSubscriber indicates a missing subscriber field.
	ErrMissingSubscriber = errors.New("missing subscriber")
	// ErrMissingClient indicates a missing client field.
	ErrMissingClient = errors.New("missing client")
	// ErrMissingIPC indicates a missing ipc field.
	ErrMissingIPC = errors.New("missing ipc")
	// ErrMissingPrefix indicates a missing prefix field.
	ErrMissingPrefix = errors.New("missing prefix")
	// ErrMissingOffset indicates a missing offset field.
	ErrMissingOffset = errors.New("missing offset")
	// ErrMissingTopics indicates a missing topics field.
	ErrMissingTopics = errors.New("missing topics")
	// ErrMissingErrorKind indicates a missing error kind.
	ErrMissingErrorKind = errors.New("missing error kind")
	// ErrMissingErrorReason indicates a missing error reason.
	ErrMissingErrorReason = errors.New("missing error reason")
	// ErrInvalidMetadata indicates metadata with an incompatible type or invalid content.
	ErrInvalidMetadata = errors.New("invalid metadata")
	// ErrInvalidPayload indicates a payload with an incompatible type or invalid content.
	ErrInvalidPayload = errors.New("invalid payload")
	// ErrUnknownField indicates an unsupported JSON field in a protocol frame.
	ErrUnknownField = errors.New("unknown JSON field")
	// ErrUnknownOpcode indicates that an operation code is unsupported.
	ErrUnknownOpcode = errors.New("unknown opcode")
	// ErrUnknownEmitter indicates that a sender type is unsupported.
	ErrUnknownEmitter = errors.New("unknown emitter")
	// ErrMissingClientID indicates that client metadata has no client ID.
	ErrMissingClientID = errors.New("missing clientId")
	// ErrMissingClientIPC indicates that client metadata has no IPC endpoint.
	ErrMissingClientIPC = errors.New("missing clientIPC")
	// ErrMissingMessage indicates a missing message field.
	ErrMissingMessage = errors.New("missing message")
	// ErrMissingMessageKey indicates that a message has no key.
	ErrMissingMessageKey = errors.New("missing message key")
	// ErrMissingMessageValue indicates that a message has no value.
	ErrMissingMessageValue = errors.New("missing message value")
	// ErrMissingMessageOffset indicates a missing delivery offset.
	ErrMissingMessageOffset = errors.New("missing message offset")
	// ErrMessageTooLarge indicates that a message exceeds MessageSize bytes.
	ErrMessageTooLarge = fmt.Errorf("message exceeds %d bytes", MessageSize)
	// ErrAdditionalJSONValue indicates that a frame contains another JSON value.
	ErrAdditionalJSONValue = errors.New("unexpected additional JSON value")
)
