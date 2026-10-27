package lifecycle_test

import (
	"errors"
	"testing"

	"tree_nity/src/exitcode"
	"tree_nity/src/lifecycle"
)

func TestNewExitResultWithoutError(t *testing.T) {
	res := lifecycle.NewExitResult(exitcode.Success, nil)

	if got := res.Code(); got != exitcode.Success {
		t.Errorf("NewExitResult(%d, nil).Code() = %d, want %d", exitcode.Success, got, exitcode.Success)
	}
	if got := res.Err(); got != nil {
		t.Errorf("NewExitResult(%d, nil).Err() = %v, want nil", exitcode.Success, got)
	}
}

func TestNewExitResultWithError(t *testing.T) {
	errReceive := errors.New("receive failed")

	res := lifecycle.NewExitResult(exitcode.IPC, errReceive)

	if got := res.Code(); got != exitcode.IPC {
		t.Errorf("NewExitResult(%d, %v).Code() = %d, want %d", exitcode.IPC, errReceive, got, exitcode.IPC)
	}
	if got := res.Err(); !errors.Is(got, errReceive) {
		t.Errorf("NewExitResult(%d, %v).Err() = %v, want %v", exitcode.IPC, errReceive, got, errReceive)
	}
}
