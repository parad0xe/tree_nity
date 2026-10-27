# Go Unit Tests

Rules for writing unit tests in Go.

## Requirements

### MUST

- A table row MUST be named with `t.Run`.

### MUST NOT

- `t.Fatalf` MUST NOT be used when the test can continue; use `t.Errorf`
  instead.

## Recommendations

### SHOULD

- A test SHOULD use the package's own name to reach unexported details, and the
  `<package>_test` name to test the external view.
- A test SHOULD check the zero value when it is intended to be usable.

## Examples

```go
func TestParseDigits(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{name: "single digit", input: "7", want: 7},
		{name: "several digits", input: "42", want: 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input)
			if err != nil || got != tt.want {
				t.Errorf("Parse(%q) = %d, %v, want %d, nil", tt.input, got, err, tt.want)
			}
		})
	}
}

func TestParseRejectsEmptyInput(t *testing.T) {
	if _, err := Parse(""); !errors.Is(err, ErrEmpty) {
		t.Errorf("Parse(%q) error = %v, want %v", "", err, ErrEmpty)
	}
}
```
