# Go Errors

Rules for wrapping errors in Go.

## Requirements

### MUST

- An error MUST be wrapped with `fmt.Errorf("...: %w", err)` when context is
  added.

## Recommendations

### SHOULD

- An error message SHOULD be lowercase and without trailing punctuation.

## Examples

```go
if err != nil {
	return fmt.Errorf("open cache %q: %w", name, err)
}
```
