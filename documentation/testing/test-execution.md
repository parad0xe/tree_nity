# Test Execution

Rules for running tests and interpreting their results.

## Requirements

### MUST

- A concurrency change MUST pass the checks required by its language-specific
  rules and [project profile](../project/README.md).

### MUST NOT

- A successful run without tests MUST NOT be reported as behavioral coverage.

## Recommendations

### SHOULD

- Tests SHOULD be run without cache when diagnosing nondeterminism.
