package main

import "errors"

var (
	// errPrintIPCPath reports that the server cannot print its IPC identifier.
	errPrintIPCPath = errors.New("cannot print the IPC identifier")
	// errUnhandledOpcode reports a request whose operation the server does not handle.
	errUnhandledOpcode = errors.New("unhandled opcode")
)
