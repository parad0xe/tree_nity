package lifecycle

// ExitResult is an exit code with the error that caused it.
type ExitResult struct {
	code int
	err  error
}

// NewExitResult returns an ExitResult with code and err.
func NewExitResult(code int, err error) ExitResult {
	return ExitResult{
		code: code,
		err:  err,
	}
}

// Code returns the exit code.
func (r ExitResult) Code() int {
	return r.code
}

// Err returns the error that caused the exit code, or nil.
func (r ExitResult) Err() error {
	return r.err
}
