package commands

import "errors"

var (
	// errMissingArgument reports a command line without a required argument.
	errMissingArgument = errors.New("missing argument")
	// errInvalidOption reports an option that the command does not accept.
	errInvalidOption = errors.New("invalid option")
	// errTooManyArguments reports more arguments than the command accepts.
	errTooManyArguments = errors.New("too many arguments")
	// errUnexpectedEmitter reports a reply that the server did not send.
	errUnexpectedEmitter = errors.New("unexpected emitter")
	// errUnexpectedOpcode reports a reply with another opcode than the request expects.
	errUnexpectedOpcode = errors.New("unexpected opcode")
	// errUnexpectedPayload reports a payload whose type does not match its opcode.
	errUnexpectedPayload = errors.New("unexpected payload")
	// errServerShuttingDown reports a request refused because the server is stopping.
	errServerShuttingDown = errors.New("server shutting down")
	// errSubscriptionEnded reports a subscription that the server ended.
	errSubscriptionEnded = errors.New("subscription ended by the server")
	// errRequestRejected reports a request that the server refused.
	errRequestRejected = errors.New("request rejected")
	// errInvalidLine reports a text message without a key and value separator.
	errInvalidLine = errors.New("invalid line")
	// errIncompleteRecord reports a binary record cut before its end.
	errIncompleteRecord = errors.New("incomplete record")
	// errEmptyKey reports a message without a key.
	errEmptyKey = errors.New("empty key")
	// errEmptyValue reports a message without a value.
	errEmptyValue = errors.New("empty value")
)
