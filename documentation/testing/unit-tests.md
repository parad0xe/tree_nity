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
- Generated test data MUST NOT be committed.

## Recommendations

### SHOULD

- A test SHOULD live next to the code it tests.
