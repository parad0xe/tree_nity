package id_test

import (
	"errors"
	"strings"
	"testing"

	"tree_nity/src/ipc/id"
)

// TestIDValidate checks format boundaries and the public validation error.
func TestIDValidate(t *testing.T) {
	tests := []struct {
		name    string
		input   id.ID
		wantErr error
	}{
		{name: "empty", wantErr: id.ErrInvalidID},
		{name: "minimum length", input: "a"},
		{name: "allowed characters", input: "Az09_.-"},
		{name: "maximum length", input: id.ID(strings.Repeat("a", 32))},
		{name: "too long", input: id.ID(strings.Repeat("a", 33)), wantErr: id.ErrInvalidID},
		{name: "slash", input: "client/name", wantErr: id.ErrInvalidID},
		{name: "space", input: "client name", wantErr: id.ErrInvalidID},
		{name: "leading newline", input: "\nclient", wantErr: id.ErrInvalidID},
		{name: "trailing newline", input: "client\n", wantErr: id.ErrInvalidID},
		{name: "non-ASCII", input: "café", wantErr: id.ErrInvalidID},
		{name: "null byte", input: "client\x00", wantErr: id.ErrInvalidID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.input.Validate(); !errors.Is(err, tt.wantErr) {
				t.Errorf("ID(%q).Validate() error = %v, want %v", tt.input, err, tt.wantErr)
			}
		})
	}
}
