package commands

import "errors"

var (
	// errMissingArgument reports a command line without a required argument.
	errMissingArgument = errors.New("missing argument")
	// errInvalidOption reports an option that the command does not accept.
	errInvalidOption = errors.New("invalid option")
	// errTooManyArguments reports more arguments than the command accepts.
	errTooManyArguments = errors.New("too many arguments")
)
