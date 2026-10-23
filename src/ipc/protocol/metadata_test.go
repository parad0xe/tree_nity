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
		emitter emitter
		input   string
		want    Metadata
	}{
		{
			name:    "client",
			emitter: client,
			input:   `{"clientId":"client-1","clientIPC":"/client-1"}`,
			want:    &ClientMetadata{clientId: "client-1", clientIPC: "/client-1"},
		},
		{
			name:    "empty client strings",
			emitter: client,
			input:   `{"clientId":"","clientIPC":""}`,
			want:    &ClientMetadata{},
		},
		{
			name:    "minimal client strings",
			emitter: client,
			input:   `{"clientId":"c","clientIPC":"q"}`,
			want:    &ClientMetadata{clientId: "c", clientIPC: "q"},
		},
		{
			name:    "large client strings",
			emitter: client,
			input:   fmt.Sprintf(`{"clientId":%q,"clientIPC":%q}`, strings.Repeat("c", 4096), strings.Repeat("q", 4096)),
			want:    &ClientMetadata{clientId: strings.Repeat("c", 4096), clientIPC: strings.Repeat("q", 4096)},
		},
		{name: "server", emitter: server, input: `{}`, want: &ServerMetadata{}},
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
		emitter emitter
		input   string
		wantErr error
	}{
		{name: "negative emitter", emitter: -1, input: `{}`, wantErr: ErrUnknownEmitter},
		{name: "unknown emitter", emitter: 2, input: `{}`, wantErr: ErrUnknownEmitter},
		{name: "empty input", emitter: client},
		{name: "whitespace", emitter: server, input: " \n\t"},
		{name: "malformed JSON", emitter: client, input: `{`},
		{name: "client array", emitter: client, input: `[]`},
		{name: "server array", emitter: server, input: `[]`},
		{name: "string", emitter: server, input: `"metadata"`},
		{name: "number", emitter: client, input: `1`},
		{name: "boolean", emitter: server, input: `false`},
		{name: "clientId has wrong type", emitter: client, input: `{"clientId":1,"clientIPC":"/client"}`},
		{name: "clientIPC has wrong type", emitter: client, input: `{"clientId":"client","clientIPC":false}`},
		{name: "unknown client field", emitter: client, input: `{"clientId":"client","clientIPC":"/client","extra":true}`},
		{name: "unknown server field", emitter: server, input: `{"extra":true}`},
		{name: "client metadata from server", emitter: server, input: `{"clientId":"client","clientIPC":"/client"}`},
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

// TestClientMetadataJSONToMetadata checks missing and null client fields.
func TestClientMetadataJSONToMetadata(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "missing fields", input: `{}`, wantErr: ErrMissingClientID},
		{name: "missing clientId", input: `{"clientIPC":"/client"}`, wantErr: ErrMissingClientID},
		{name: "null clientId", input: `{"clientId":null,"clientIPC":"/client"}`, wantErr: ErrMissingClientID},
		{name: "missing clientIPC", input: `{"clientId":"client"}`, wantErr: ErrMissingClientIPC},
		{name: "null clientIPC", input: `{"clientId":"client","clientIPC":null}`, wantErr: ErrMissingClientIPC},
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
