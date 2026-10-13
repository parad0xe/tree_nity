# Go Formatting

Rules for formatting Go code and grouping imports.

## Requirements

### MUST

- A changed Go file MUST be formatted with `gofmt`.
- Imports MUST be grouped as standard library, third-party, then module-local,
  separated by blank lines.

### MUST NOT

- A formatting change MUST NOT touch files outside the change.

## Examples

```go
import (
	"fmt"
	"os"

	"github.com/google/uuid"

	"example.com/project/internal/parser"
)
```
