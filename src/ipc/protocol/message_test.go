package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// TestMessageJSONToMessage checks required fields, byte limits, and conversion.
func TestMessageJSONToMessage(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Message
		wantErr string
	}{
		{name: "missing fields", input: `{}`, wantErr: "missing message key"},
		{name: "missing key", input: `{"value":"v"}`, wantErr: "missing message key"},
		{name: "null key", input: `{"key":null,"value":"v"}`, wantErr: "missing message key"},
		{name: "missing value", input: `{"key":"k"}`, wantErr: "missing message value"},
		{name: "null value", input: `{"key":"k","value":null}`, wantErr: "missing message value"},
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
			name:    "oversized key",
			input:   fmt.Sprintf(`{"key":%q,"value":""}`, strings.Repeat("k", 1025)),
			wantErr: "message exceeds 1024 bytes",
		},
		{
			name:    "oversized value",
			input:   fmt.Sprintf(`{"key":"","value":%q}`, strings.Repeat("v", 1025)),
			wantErr: "message exceeds 1024 bytes",
		},
		{
			name:    "combined size exceeds limit",
			input:   fmt.Sprintf(`{"key":%q,"value":%q}`, strings.Repeat("k", 512), strings.Repeat("v", 513)),
			wantErr: "message exceeds 1024 bytes",
		},
		{
			name:  "UTF-8 at byte limit",
			input: fmt.Sprintf(`{"key":"","value":%q}`, strings.Repeat("é", 512)),
			want:  Message{value: strings.Repeat("é", 512)},
		},
		{
			name:    "UTF-8 exceeds byte limit",
			input:   fmt.Sprintf(`{"key":"k","value":%q}`, strings.Repeat("é", 512)),
			wantErr: "message exceeds 1024 bytes",
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
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Errorf("toMessage(%s) error = %v, want %q", tt.input, err, tt.wantErr)
				}
			} else if err != nil {
				t.Errorf("toMessage(%s) error = %v, want nil", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("toMessage(%s) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}
