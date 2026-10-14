# Go Unit Tests

Rules for writing unit tests in Go.

## Requirements

### MUST

- A test with several cases MUST name each case with `t.Run`.

### MUST NOT

- `t.Fatalf` MUST NOT be used when the test can continue; use `t.Errorf`
  instead.

## Recommendations

### SHOULD

- A test of several inputs for one contract SHOULD be table-driven.
- A test SHOULD use the package's own name to reach unexported details, and the
  `<package>_test` name to test the external view.
- A test SHOULD check the zero value when it is intended to be usable.

## Examples

```go
func TestParse(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{name: "empty", input: "", want: 0},
		{name: "single digit", input: "7", want: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Parse(tt.input); got != tt.want {
				t.Errorf("Parse(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}
```
