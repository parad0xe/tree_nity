# Go Test Execution

Rules for running Go tests.

## Requirements

### MUST NOT

- A package reporting `[no test files]` MUST NOT be counted as tested.

## Running tests without cache

Bypass cached results when diagnosing nondeterminism:

```bash
go test -count=1 ./...
```
