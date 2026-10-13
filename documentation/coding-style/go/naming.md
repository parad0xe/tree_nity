# Go Naming

Rules for naming Go identifiers, packages, and files.

## Requirements

### MUST

- An identifier MUST use `MixedCaps` or `mixedCaps`.
- An initialism MUST keep a consistent case, such as `ID`, `URL`, or `HTTP`.
- A receiver name MUST be short and consistent across the methods of its type.
- A package name MUST be lowercase and without underscores.
- A file name MUST be lowercase.

### MUST NOT

- An identifier MUST NOT be exported unless another package needs it.
- A receiver MUST NOT be named `this` or `self`.
- A getter MUST NOT use the `Get` prefix.

## Recommendations

### SHOULD

- An interface SHOULD be defined next to the code that consumes it.

## Examples

```go
package parser

type Parser struct{}

func (p *Parser) State() State

var userID string
```
