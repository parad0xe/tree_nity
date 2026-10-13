# Go Errors

Rules for wrapping errors in Go.

## Requirements

### MUST

- An error whose identity is part of the interface's contract MUST remain
  inspectable when context is added, using wrapping such as
  `fmt.Errorf("...: %w", err)`.

## Recommendations

### SHOULD

- An error message SHOULD be lowercase and without trailing punctuation.
- Error wrapping SHOULD expose only errors that callers are intended to inspect
  with `errors.Is` or `errors.As`.

## Examples

Here, the caller is intended to inspect the underlying file error. When that
error is an implementation detail, translate it into an error from the
interface's contract instead of exposing it through wrapping.

```go
if err != nil {
	return fmt.Errorf("open cache %q: %w", name, err)
}
```
