# Go Comments

Rules for documenting exported Go identifiers.

## Requirements

### MUST

- An exported identifier MUST have a doc comment starting with its name.

## Examples

```go
// Cache stores entries by key. It is safe for concurrent use.
type Cache struct{}
```
