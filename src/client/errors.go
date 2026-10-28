package main

import "errors"

var (
	// errMissingIPCIdentifier reports a command line without the server IPC identifier.
	errMissingIPCIdentifier = errors.New("missing IPC identifier")
	// errMissingCommand reports a command line without a command after the IPC identifier.
	errMissingCommand = errors.New("missing command name")
	// errUnknownCommand reports a command that the client does not handle.
	errUnknownCommand = errors.New("unknown command")
	// errCommandNotStopped reports a command that did not stop once cancelled.
	errCommandNotStopped = errors.New("command did not stop once cancelled")
)
