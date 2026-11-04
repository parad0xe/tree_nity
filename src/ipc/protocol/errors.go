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
	// ErrMissingTopic indicates that a request payload has no topic field.
	ErrMissingTopic = errors.New("missing topic")
	// ErrMissingPrefix indicates that a subscription request has no prefix field.
	ErrMissingPrefix = errors.New("missing prefix")
	// ErrMissingOffset indicates that a payload has no required offset field.
	ErrMissingOffset = errors.New("missing offset")
	// ErrMissingTopics indicates that a topic list response has no topics field.
	ErrMissingTopics = errors.New("missing topics")
	// ErrMissingErrorKind indicates that an error reply has no kind.
	ErrMissingErrorKind = errors.New("missing error kind")
	// ErrMissingErrorReason indicates that an error reply has no reason.
	ErrMissingErrorReason = errors.New("missing error reason")
	// ErrInvalidMetadata indicates metadata with an incompatible type or invalid content.
	ErrInvalidMetadata = errors.New("invalid metadata")
	// ErrInvalidPayload indicates a payload with an incompatible type or invalid content.
	ErrInvalidPayload = errors.New("invalid payload")
	// ErrUnknownOpcode indicates that an operation code is unsupported.
	ErrUnknownOpcode = errors.New("unknown opcode")
	// ErrUnknownEmitter indicates that a sender type is unsupported.
	ErrUnknownEmitter = errors.New("unknown emitter")
	// ErrMissingClientID indicates that client metadata has no client ID.
	ErrMissingClientID = errors.New("missing clientId")
	// ErrMissingClientIPC indicates that client metadata has no IPC endpoint.
	ErrMissingClientIPC = errors.New("missing clientIPC")
	// ErrMissingMessage indicates that a payload has no message field.
	ErrMissingMessage = errors.New("missing message")
	// ErrMissingMessageKey indicates that a message has no key.
	ErrMissingMessageKey = errors.New("missing message key")
	// ErrMissingMessageValue indicates that a message has no value.
	ErrMissingMessageValue = errors.New("missing message value")
	// ErrMissingMessageOffset indicates that a delivered message has no offset.
	ErrMissingMessageOffset = errors.New("missing message offset")
	// ErrMessageTooLarge indicates that a message exceeds MessageSize bytes.
	ErrMessageTooLarge = fmt.Errorf("message exceeds %d bytes", MessageSize)
	// ErrAdditionalJSONValue indicates that a frame contains another JSON value.
	ErrAdditionalJSONValue = errors.New("unexpected additional JSON value")
)
