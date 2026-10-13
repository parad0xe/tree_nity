# Testing and Validation

This domain is the entry point for Tree_nity's executable validation
procedures. Each procedure has one authoritative document:

- [Formatting and static analysis](formatting.md) — format changed Go files and
  run repository-wide formatting and vet checks.
- [Test execution](test-execution.md) — select and run the appropriate Go and
  subject tests, then interpret and report the results.
- [Unit tests](unit-tests.md) — structure unit tests and cover the subject's
  prefix-matching requirements.

The project subject requires Go's native testing framework and comprehensive
unit testing of prefix matching. The documents above separate how validation
is executed from how test cases are designed.

## Current Makefile behavior

The Makefile must be treated as executable truth, not inferred from target
names:

- `test` delegates directly to `go test ./...`.
- `all` depends on `server` and `client`.
- `server` runs `go build -o src/server ./server`.
- `client` runs `go build -o src/client ./client`.

The current packages are under `src/server` and `src/client`, while those build
commands address `./server` and `./client`. Consequently, the default build
currently fails before proving that either executable can be built. Do not
silently replace the required Makefile result with a different command; an
additional diagnostic command may be reported separately.
