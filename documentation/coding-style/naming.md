# Naming

Rules for choosing names.

## Requirements

### MUST

- A domain term MUST keep the same name across the codebase.
- A renamed public identifier MUST be treated as an interface change.

### MUST NOT

- A name MUST NOT repeat the context given by its module, type, or receiver.

## Recommendations

### SHOULD NOT

- A new module SHOULD NOT use a vague name such as `util`, `common`, or `misc`.
