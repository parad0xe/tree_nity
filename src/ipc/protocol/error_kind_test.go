package protocol

import (
	"errors"
	"testing"
)

// TestErrorKindValues fixes the numeric categories used on the wire.
func TestErrorKindValues(t *testing.T) {
	tests := []struct {
		name string
		kind ErrorKind
		want int
	}{
		{name: "invalid request", kind: InvalidRequest, want: 0},
		{name: "topic not found", kind: TopicNotFound, want: 1},
		{name: "topic exists", kind: TopicExists, want: 2},
		{name: "client not found", kind: ClientNotFound, want: 3},
		{name: "client name taken", kind: ClientNameTaken, want: 4},
		{name: "server shutting down", kind: ServerShuttingDown, want: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := int(tt.kind); got != tt.want {
				t.Errorf("int(%s) = %d, want %d", tt.name, got, tt.want)
			}
		})
	}
}

// TestErrorKindValidate checks each supported rejection category.
func TestErrorKindValidate(t *testing.T) {
	tests := []struct {
		name string
		kind ErrorKind
	}{
		{name: "invalid request", kind: InvalidRequest},
		{name: "topic not found", kind: TopicNotFound},
		{name: "topic exists", kind: TopicExists},
		{name: "client not found", kind: ClientNotFound},
		{name: "client name taken", kind: ClientNameTaken},
		{name: "server shutting down", kind: ServerShuttingDown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.kind.validate(); err != nil {
				t.Errorf("ErrorKind(%d).validate() error = %v, want nil", tt.kind, err)
			}
		})
	}
}

// TestErrorKindValidateRejectsUnknownValues checks unsupported categories.
func TestErrorKindValidateRejectsUnknownValues(t *testing.T) {
	tests := []struct {
		name string
		kind ErrorKind
	}{
		{name: "negative", kind: -1},
		{name: "first unknown", kind: ServerShuttingDown + 1},
		{name: "large", kind: 256},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.kind.validate(); !errors.Is(err, ErrInvalidPayload) {
				t.Errorf("ErrorKind(%d).validate() error = %v, want %v", tt.kind, err, ErrInvalidPayload)
			}
		})
	}
}
