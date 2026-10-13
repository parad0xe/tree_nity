# Go Conventions

These are repository conventions unless a rule explicitly refers to the
project subject. They favor standard Go behavior and keep formatting mechanical.

## Formatting and imports

- Format every changed Go file with `gofmt`.
- Do not hand-align code or preserve formatting that `gofmt` removes.
- Let `gofmt` sort imports within groups. When multiple groups are needed, use
  standard-library, third-party, then Tree_nity imports, separated by blank
  lines.
- Remove unused imports. Do not add a formatting or import dependency solely to
  replace standard Go tooling.

Apply and verify these rules with the project-specific
[formatting and static-analysis procedure](../testing/formatting.md).

## Packages and files

- Keep packages focused and preserve the current source organization under
  `src/` unless package restructuring is the assigned task.
- A directory's Go files must use one package name, apart from the conventional
  external test package form.
- Package names should be short, lowercase, and meaningful at call sites.
- Keep related declarations together. Create a new file when it gives a clear
  responsibility, not merely to keep files short.
- Use `_test.go` only for tests and platform suffixes only when the build really
  depends on that platform.

Some current directory names are more specific than their package names—for
example, protocol subdirectories declare `package protocol`, and session
subdirectories declare `package session`. Treat that as existing organization,
not an invitation to rename packages during unrelated work.

## Errors

- Return errors for expected runtime failures; do not use panic as ordinary
  control flow.
- Handle an error at the level that has enough context to act on it. Otherwise,
  return it to the caller.
- Add useful context with `fmt.Errorf("...: %w", err)` so callers can still use
  `errors.Is` or `errors.As`.
- Do not ignore errors silently. If an error is intentionally irrelevant,
  express and explain that decision at the call site when it is not obvious.
- Keep error messages concise, lowercase, and free of trailing punctuation when
  they are intended to be wrapped into a larger message.

## Comments and documentation

- Explain intent, invariants, ownership, protocol constraints, and surprising
  decisions; do not narrate obvious syntax.
- Keep comments accurate when changing the code they describe.
- Document exported identifiers when their purpose, contract, concurrency
  behavior, or zero value is not self-evident. Start a public declaration's
  documentation with its identifier where normal Go documentation style calls
  for it.
- Use `TODO` only for concrete follow-up work and include enough context to make
  it actionable.

## Dependencies and interfaces

Prefer the standard library and existing module dependencies. A new dependency
must solve a concrete need and should be assessed for maintenance and license
impact. Define small interfaces around behavior that has multiple
implementations or a real testing boundary; do not introduce an interface only
in anticipation of possible future use.

Interface and externally observable behavior changes must be called out in the
plan, tests, documentation, and delivery report. Never hide one inside a
cleanup.

## Concurrency

Tree_nity is a message queue and its channels and shared state require explicit
ownership.

- State who creates, writes to, closes, and reads from a channel when ownership
  is not obvious.
- Only the sending or owning side should close a channel; account for repeated
  close and send-after-close paths.
- Protect shared mutable state consistently and keep critical sections small.
- Avoid holding a lock while performing blocking channel or I/O operations
  unless the invariant specifically requires it.
- Give goroutines a defined termination path and avoid leaks on errors or
  shutdown.
- Add deterministic synchronization in tests instead of relying on sleeps.
- Run the race detector for concurrency-related changes as described in the
  [testing guide](../testing/README.md).
