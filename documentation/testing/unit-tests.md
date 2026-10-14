# Unit Tests

Rules for writing unit tests.

## Requirements

### MUST

- A behavior change MUST come with tests that cover it.
- A data-structure test MUST cover empty, minimal, and large inputs.
- A concurrent test MUST complete deterministically and release its resources.
- A failure message MUST state the operation, the input, the expected value, and
  the actual value.

### MUST NOT

- A test MUST NOT use sleeps to synchronize.
- Temporary data generated during a test run MUST NOT be committed.

## Recommendations

### SHOULD

- A test SHOULD live next to the code it tests.
- A generated fixture SHOULD be committed only when needed to reproduce a
  regression or define expected behavior, with its provenance documented.
