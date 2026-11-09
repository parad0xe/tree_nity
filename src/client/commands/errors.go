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
	// errRequestRejected reports a request that the server refused.
	errRequestRejected = errors.New("request rejected")
)
