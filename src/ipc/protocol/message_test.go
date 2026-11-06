package protocol

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
		{name: "minimal key", input: `{"key":"aw==","value":""}`, want: Message{key: []byte("k")}},
		{name: "minimal value", input: `{"key":"","value":"dg=="}`, want: Message{value: []byte("v")}},
		{name: "message", input: `{"key":"a2V5","value":"dmFsdWU="}`, want: Message{key: []byte("key"), value: []byte("value")}},
		{name: "binary bytes", input: `{"key":"dXNlcg==","value":"AP8Q"}`, want: Message{key: []byte("user"), value: []byte{0x00, 0xff, 0x10}}},
		{
			name:  "key at byte limit",
			input: fmt.Sprintf(`{"key":%q,"value":""}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 1024)))),
			want:  Message{key: []byte(strings.Repeat("k", 1024))},
		},
		{
			name:  "value at byte limit",
			input: fmt.Sprintf(`{"key":"","value":%q}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("v", 1024)))),
			want:  Message{value: []byte(strings.Repeat("v", 1024))},
		},
		{
			name:  "combined byte limit",
			input: fmt.Sprintf(`{"key":%q,"value":%q}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 512))), base64.StdEncoding.EncodeToString([]byte(strings.Repeat("v", 512)))),
			want:  Message{key: []byte(strings.Repeat("k", 512)), value: []byte(strings.Repeat("v", 512))},
		},
		{
			name:  "UTF-8 at byte limit",
			input: fmt.Sprintf(`{"key":"","value":%q}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("é", 512)))),
			want:  Message{value: []byte(strings.Repeat("é", 512))},
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
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toMessage(%s) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

// TestMessageJSONToMessageRejectsInvalidInput checks required fields, base64, and decoded sizes.
func TestMessageJSONToMessageRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "missing fields", input: `{}`, wantErr: ErrMissingMessageKey},
		{name: "missing key", input: `{"value":"dg=="}`, wantErr: ErrMissingMessageKey},
		{name: "null key", input: `{"key":null,"value":"dg=="}`, wantErr: ErrMissingMessageKey},
		{name: "missing value", input: `{"key":"aw=="}`, wantErr: ErrMissingMessageValue},
		{name: "null value", input: `{"key":"aw==","value":null}`, wantErr: ErrMissingMessageValue},
		{name: "invalid base64 key", input: `{"key":"?","value":""}`, wantErr: ErrInvalidPayload},
		{name: "invalid base64 value", input: `{"key":"","value":"?"}`, wantErr: ErrInvalidPayload},
		{name: "missing base64 padding", input: `{"key":"aw","value":""}`, wantErr: ErrInvalidPayload},
		{name: "truncated base64 padding", input: `{"key":"aw=","value":""}`, wantErr: ErrInvalidPayload},
		{
			name:    "oversized key",
			input:   fmt.Sprintf(`{"key":%q,"value":""}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 1025)))),
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "oversized value",
			input:   fmt.Sprintf(`{"key":"","value":%q}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("v", 1025)))),
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "combined size exceeds limit",
			input:   fmt.Sprintf(`{"key":%q,"value":%q}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 512))), base64.StdEncoding.EncodeToString([]byte(strings.Repeat("v", 513)))),
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "UTF-8 exceeds byte limit",
			input:   fmt.Sprintf(`{"key":"aw==","value":%q}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("é", 512)))),
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
			if !reflect.DeepEqual(got, Message{}) {
				t.Errorf("toMessage(%s) = %+v, want zero Message", tt.input, got)
			}
		})
	}
}
