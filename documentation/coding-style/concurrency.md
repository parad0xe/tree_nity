# Concurrency

Rules for sharing state between concurrent tasks.

## Requirements

### MUST

- Shared mutable state MUST be accessed under synchronization.
- A concurrent task MUST have a defined termination path, including on error
  and shutdown.

### MUST NOT

- A lock MUST NOT be held during a blocking operation unless an invariant
  requires it.
