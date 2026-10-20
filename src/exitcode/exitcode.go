package exitcode

const (
	// Success reports that the process completed without error.
	Success = 0
	// General reports invalid arguments or a connection failure.
	General = 1
	// Topic reports a topic or client error.
	Topic = 2
	// IPC reports an IPC communication error.
	IPC = 3
)
