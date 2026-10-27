# Unit Tests

Rules for writing unit tests.

## Requirements

### MUST

- A behavior change MUST come with tests that cover it.
- A test function MUST check a single behavior, stated by its name.
- Cases sharing a test function MUST form a table whose rows run the same
  setup, call, and assertions, and differ only by their values.
- A data structure MUST be tested with empty, minimal, and large inputs.
- A concurrent test MUST complete deterministically and release its resources.
- A failure message MUST state the operation, the input, the expected value, and
  the actual value.

### MUST NOT

- A test MUST NOT use sleeps to synchronize.
- A table MUST NOT mix cases that succeed with cases that fail.
- Temporary data generated during a test run MUST NOT be committed.

## Recommendations

### SHOULD

- A test SHOULD live next to the code it tests.
- A generated fixture SHOULD be committed only when needed to reproduce a
  regression or define expected behavior, with its provenance documented.
