# Test Execution

This is the executable test procedure for Tree_nity. Test design and required
prefix-matching cases are defined separately in [Unit tests](unit-tests.md).

## Repository test entry point

Run the Makefile target selected by the repository workflow:

```bash
make test
```

The current target delegates directly to:

```bash
go test ./...
```

It compiles every discovered package and runs its Go tests. When the **Test**
step is selected, `make test` must exit with code 0. Running `go test ./...`
directly is useful when diagnosing package-level failures, but does not provide
different coverage from the current Makefile target.

Go caches successful test results. When a fresh execution matters while
diagnosing nondeterminism, use:

```bash
go test -count=1 ./...
```

This cache-bypassing command is a repository recommendation, not a subject
requirement.

## Race detector

For changes involving goroutines, channels, synchronization, shared state, or
coordination between prefixes and consumers, also run:

```bash
go test -race ./...
```

Run it before integration when the platform supports the Go race detector.

## Subject scenario

Changes affecting the command-line interface, standard output, or IPC must also
run the test scenario defined by the project subject. Record every manual step,
input, expected result, and actual result so another contributor can reproduce
the scenario.

## Interpreting results

At the time this procedure was established, the repository contained no
`*_test.go` files. A successful command that reports `[no test files]` proves
package compilation, not behavioral coverage. It does not establish that
prefix matching or any other feature is tested.

For every delivery, report:

- each command run and whether it passed, failed, or was skipped;
- the first actionable failure and affected package or test;
- whether relevant test files and cases exist;
- race-detector and subject-scenario status when applicable; and
- environment limitations and behavior that remains untested.

Documentation-only changes require link and content review. Go test results may
provide a repository baseline, but do not validate Markdown semantics.
