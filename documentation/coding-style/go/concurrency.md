# Go Concurrency

Rules for channel ownership and cancellation.

## Requirements

### MUST

- A channel MUST be closed only by its designated owner, after all sends have
  completed and no further send can start.
- A channel MUST be closed at most once.
- A blocking channel operation MUST have a defined termination path, through
  cancellation or a guarantee that its peer completes the operation.

### MUST NOT

- A send MUST NOT race with channel closure or target a closed channel.
- A nil channel MUST NOT be closed.

## Examples

The producer is the sole sender and closer of `out`. The caller passes a non-nil
channel and cancels `ctx` if the consumer stops receiving. With multiple senders,
the designated closer waits for all senders to finish before closing the channel.

```go
import "context"

func produce(ctx context.Context, out chan<- int) {
	defer close(out)
	for i := range 3 {
		select {
		case out <- i:
		case <-ctx.Done():
			return
		}
	}
}
```
