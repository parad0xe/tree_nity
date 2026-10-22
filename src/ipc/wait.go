package ipc

import (
	"context"
	"time"
)

// waitSlice is the longest a queue operation waits before it checks its
// context again.
const waitSlice = 100 * time.Millisecond

// sliceDeadline returns the end of the next wait: waitSlice from now, or the
// deadline of ctx when it comes first.
func sliceDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(waitSlice)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}

	return deadline
}
