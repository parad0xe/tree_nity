# Go Concurrency

Rules for closing channels.

## Requirements

### MUST

- A channel MUST be closed only by its sending or owning side.
- A channel MUST be closed at most once.

### MUST NOT

- A closed channel MUST NOT receive a send.

## Examples

```go
func produce(out chan<- int) {
	defer close(out)
	for i := range 3 {
		out <- i
	}
}
```
