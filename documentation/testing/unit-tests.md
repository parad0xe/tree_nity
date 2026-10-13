# Unit Tests

The project subject requires Go's native `testing` framework and comprehensive
unit testing for prefix matching. This document describes the expected test
shape without claiming that the currently empty prefix-tree implementation or
its tests already exist.

## Structure

- Put tests beside the code as `<name>_test.go`.
- Name top-level tests `TestXxx` and use descriptive `t.Run` cases.
- Prefer table-driven tests when several inputs exercise one contract.
- Use the package's own name when tests need unexported implementation details;
  use `<package>_test` when an external consumer's view is the behavior under
  test.
- Keep fixtures small and explicit. Generate large datasets deterministically
  in the test rather than committing generated artifacts.
- Test public behavior and important invariants, not a private implementation
  shape that may change safely.

## Prefix-matching coverage

The prefix-matching suite must cover the subject's contract for at least:

- exact prefix matches;
- partial prefix matches;
- prefixes that do not match;
- empty-prefix behavior;
- every implemented data-structure operation;
- empty datasets and meaningfully large datasets;
- multiple consumers sharing the same or overlapping prefixes; and
- integration of mixed prefix types in one dataset or dispatch scenario.

For each category, include the matching result and the non-matching boundary
around it. Test duplicate inputs, ordering, removal, or update only when the
implemented API supports those operations; do not invent an operation merely
to satisfy a generic checklist.

The precise meaning of an empty or partial prefix must come from the subject
and the documented API. If those sources are ambiguous, record the ambiguity
and make the expected behavior explicit before implementing the test.

## Data structures and concurrency

Test the zero value when it is intended to be usable. Exercise initialization,
normal operation, boundary inputs, and transitions back to an empty structure.
For large datasets, choose a deterministic size that is large enough to expose
structural errors without turning a unit test into a benchmark.

Tests involving consumers or goroutines must have deterministic completion and
cleanup. Coordinate with channels, wait groups, contexts, or test deadlines;
do not use arbitrary sleeps as synchronization. Validate them with the
[race-detector procedure](test-execution.md#race-detector).

## Failure reporting

Failure messages should identify the operation, input, expected value, and
actual value. Use `t.Fatalf` only when the test cannot continue meaningfully;
otherwise use `t.Errorf` so a table can report multiple failing cases.

When reporting a failed validation, include the exact command, package and test
name, relevant failure output, and whether the problem is a product defect,
test defect, environment issue, or unresolved specification question.
