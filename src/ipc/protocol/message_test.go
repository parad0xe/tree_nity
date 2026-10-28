package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestMessageToJSONNil checks that business validation rejects a nil message without panic.
func TestMessageToJSONNil(t *testing.T) {
	var message *Message
	got, err := message.toJSON()
	if !errors.Is(err, ErrMissingMessage) {
		t.Errorf("toJSON() on a nil message error = %v, want %v", err, ErrMissingMessage)
	}
	if got != (messageJSON{}) {
		t.Errorf("toJSON() on a nil message = %+v, want an empty JSON message on error", got)
	}
}

// TestMessageJSONToMessage checks conversion up to the message byte limit.
func TestMessageJSONToMessage(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Message
	}{
		{name: "empty strings", input: `{"key":"","value":""}`, want: Message{}},
		{name: "minimal key", input: `{"key":"k","value":""}`, want: Message{key: "k"}},
		{name: "minimal value", input: `{"key":"","value":"v"}`, want: Message{value: "v"}},
		{name: "message", input: `{"key":"key","value":"value"}`, want: Message{key: "key", value: "value"}},
		{
			name:  "key at byte limit",
			input: fmt.Sprintf(`{"key":%q,"value":""}`, strings.Repeat("k", 1024)),
			want:  Message{key: strings.Repeat("k", 1024)},
		},
		{
			name:  "value at byte limit",
			input: fmt.Sprintf(`{"key":"","value":%q}`, strings.Repeat("v", 1024)),
			want:  Message{value: strings.Repeat("v", 1024)},
		},
		{
			name:  "combined byte limit",
			input: fmt.Sprintf(`{"key":%q,"value":%q}`, strings.Repeat("k", 512), strings.Repeat("v", 512)),
			want:  Message{key: strings.Repeat("k", 512), value: strings.Repeat("v", 512)},
		},
		{
			name:  "UTF-8 at byte limit",
			input: fmt.Sprintf(`{"key":"","value":%q}`, strings.Repeat("é", 512)),
			want:  Message{value: strings.Repeat("é", 512)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var input messageJSON
			if err := json.Unmarshal([]byte(tt.input), &input); err != nil {
				t.Errorf("Unmarshal(%q) error = %v, want nil", tt.input, err)
				return
			}

			got, err := input.toMessage()
			if err != nil {
				t.Errorf("toMessage(%s) error = %v, want nil", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("toMessage(%s) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

// TestMessageJSONToMessageRejectsInvalidInput checks required fields and oversized messages.
func TestMessageJSONToMessageRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "missing fields", input: `{}`, wantErr: ErrMissingMessageKey},
		{name: "missing key", input: `{"value":"v"}`, wantErr: ErrMissingMessageKey},
		{name: "null key", input: `{"key":null,"value":"v"}`, wantErr: ErrMissingMessageKey},
		{name: "missing value", input: `{"key":"k"}`, wantErr: ErrMissingMessageValue},
		{name: "null value", input: `{"key":"k","value":null}`, wantErr: ErrMissingMessageValue},
		{
			name:    "oversized key",
			input:   fmt.Sprintf(`{"key":%q,"value":""}`, strings.Repeat("k", 1025)),
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "oversized value",
			input:   fmt.Sprintf(`{"key":"","value":%q}`, strings.Repeat("v", 1025)),
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "combined size exceeds limit",
			input:   fmt.Sprintf(`{"key":%q,"value":%q}`, strings.Repeat("k", 512), strings.Repeat("v", 513)),
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "UTF-8 exceeds byte limit",
			input:   fmt.Sprintf(`{"key":"k","value":%q}`, strings.Repeat("é", 512)),
			wantErr: ErrMessageTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var input messageJSON
			if err := json.Unmarshal([]byte(tt.input), &input); err != nil {
				t.Errorf("Unmarshal(%q) error = %v, want nil", tt.input, err)
				return
			}

			got, err := input.toMessage()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("toMessage(%s) error = %v, want %v", tt.input, err, tt.wantErr)
			}
			if got != (Message{}) {
				t.Errorf("toMessage(%s) = %+v, want zero Message", tt.input, got)
			}
		})
	}
}
