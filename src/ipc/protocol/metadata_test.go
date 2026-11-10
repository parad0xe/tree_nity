package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestNewMetadataJSON checks emitter dispatch and metadata conversion.
func TestNewMetadataJSON(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		input   string
		want    Metadata
	}{
		{
			name:    "client",
			emitter: Client,
			input:   `{"clientIPC":"/client-1"}`,
			want:    &ClientMetadata{clientIPC: "/client-1"},
		},
		{
			name:    "empty client IPC",
			emitter: Client,
			input:   `{"clientIPC":""}`,
			want:    &ClientMetadata{},
		},
		{
			name:    "minimal client IPC",
			emitter: Client,
			input:   `{"clientIPC":"q"}`,
			want:    &ClientMetadata{clientIPC: "q"},
		},
		{
			name:    "large IPC",
			emitter: Client,
			input:   fmt.Sprintf(`{"clientIPC":%q}`, strings.Repeat("q", 4096)),
			want:    &ClientMetadata{clientIPC: strings.Repeat("q", 4096)},
		},
		{name: "server", emitter: Server, input: `{}`, want: &ServerMetadata{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := newMetadataJSON(tt.emitter, json.RawMessage(tt.input))
			if err != nil {
				t.Errorf("newMetadataJSON(%d, %s) error = %v, want nil", tt.emitter, tt.input, err)
				return
			}
			if decoded == nil {
				t.Errorf("newMetadataJSON(%d, %s) = nil, want a metadata decoder", tt.emitter, tt.input)
				return
			}

			got, err := decoded.toMetadata()
			if err != nil {
				t.Errorf("toMetadata(emitter=%d, input=%s) error = %v, want nil", tt.emitter, tt.input, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toMetadata(emitter=%d, input=%s) = %#v, want %#v", tt.emitter, tt.input, got, tt.want)
			}
		})
	}
}

// TestNewMetadataJSONRejectsInvalidInput checks emitters, JSON types, and unknown fields.
func TestNewMetadataJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		input   string
		wantErr error
	}{
		{name: "negative emitter", emitter: -1, input: `{}`, wantErr: ErrUnknownEmitter},
		{name: "unknown emitter", emitter: 2, input: `{}`, wantErr: ErrUnknownEmitter},
		{name: "empty input", emitter: Client},
		{name: "whitespace", emitter: Server, input: " \n\t"},
		{name: "malformed JSON", emitter: Client, input: `{`},
		{name: "client array", emitter: Client, input: `[]`},
		{name: "server array", emitter: Server, input: `[]`},
		{name: "string", emitter: Server, input: `"metadata"`},
		{name: "number", emitter: Client, input: `1`},
		{name: "boolean", emitter: Server, input: `false`},
		{name: "legacy clientId", emitter: Client, input: `{"clientId":"client-1","clientIPC":"/client"}`, wantErr: ErrUnknownField},
		{name: "clientIPC has wrong type", emitter: Client, input: `{"clientIPC":false}`},
		{name: "unknown client field", emitter: Client, input: `{"clientIPC":"/client","extra":true}`, wantErr: ErrUnknownField},
		{name: "unknown server field", emitter: Server, input: `{"extra":true}`, wantErr: ErrUnknownField},
		{name: "client metadata from server", emitter: Server, input: `{"clientIPC":"/client"}`, wantErr: ErrUnknownField},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newMetadataJSON(tt.emitter, json.RawMessage(tt.input))
			if err == nil || got != nil {
				t.Errorf("newMetadataJSON(%d, %q) = %#v, error = %v, want nil and an error", tt.emitter, tt.input, got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("newMetadataJSON(%d, %q) error = %v, want %v", tt.emitter, tt.input, err, tt.wantErr)
			}
		})
	}
}

// TestClientMetadataJSONToMetadataRejectsMissingFields checks missing and null client fields.
func TestClientMetadataJSONToMetadataRejectsMissingFields(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "missing fields", input: `{}`, wantErr: ErrMissingClientIPC},
		{name: "missing clientIPC with legacy clientId", input: `{"clientId":"client-1"}`, wantErr: ErrMissingClientIPC},
		{name: "null clientIPC with legacy clientId", input: `{"clientId":"client-1","clientIPC":null}`, wantErr: ErrMissingClientIPC},
		{name: "missing clientIPC", input: `{}`, wantErr: ErrMissingClientIPC},
		{name: "null clientIPC", input: `{"clientIPC":null}`, wantErr: ErrMissingClientIPC},
		{name: "missing clientIPC with invalid clientId", input: `{"clientId":""}`, wantErr: ErrMissingClientIPC},
		{name: "null clientIPC with invalid clientId", input: `{"clientId":"","clientIPC":null}`, wantErr: ErrMissingClientIPC},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var input clientMetadataJSON
			if err := json.Unmarshal([]byte(tt.input), &input); err != nil {
				t.Errorf("Unmarshal(%q) error = %v, want nil", tt.input, err)
				return
			}

			got, err := input.toMetadata()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("toMetadata(%s) error = %v, want %v", tt.input, err, tt.wantErr)
			}
			if got != nil {
				t.Errorf("toMetadata(%s) = %#v, want nil on error", tt.input, got)
			}
		})
	}
}
