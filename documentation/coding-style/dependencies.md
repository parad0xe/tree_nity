# Dependencies and Interfaces

Rules for adding dependencies and changing interfaces.

## Requirements

### MUST

- A new dependency MUST solve a need that the standard library and the existing
  dependencies do not cover.
- An externally visible behavior change MUST be documented and covered by
  tests.

### MUST NOT

- An abstraction MUST NOT be introduced only in anticipation of future use.

## Recommendations

### SHOULD

- A new dependency SHOULD be assessed for maintenance and license impact.
