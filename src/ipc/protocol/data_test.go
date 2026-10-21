package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// TestDataJSONValidate checks that every envelope field must be present and non-null.
func TestDataJSONValidate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "empty envelope", input: `{}`, wantErr: ErrMissingOpcode},
		{name: "missing opcode", input: `{"emitter":1,"metadata":{},"payload":{}}`, wantErr: ErrMissingOpcode},
		{name: "null opcode", input: `{"opcode":null,"emitter":1,"metadata":{},"payload":{}}`, wantErr: ErrMissingOpcode},
		{name: "missing emitter", input: `{"opcode":0,"metadata":{},"payload":{}}`, wantErr: ErrMissingEmitter},
		{name: "null emitter", input: `{"opcode":0,"emitter":null,"metadata":{},"payload":{}}`, wantErr: ErrMissingEmitter},
		{name: "missing metadata", input: `{"opcode":0,"emitter":1,"payload":{}}`, wantErr: ErrMissingMetadata},
		{name: "null metadata", input: `{"opcode":0,"emitter":1,"metadata":null,"payload":{}}`, wantErr: ErrMissingMetadata},
		{name: "missing payload", input: `{"opcode":0,"emitter":1,"metadata":{}}`, wantErr: ErrMissingPayload},
		{name: "null payload", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":null}`, wantErr: ErrMissingPayload},
		{name: "complete envelope", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":{}}`},
		{name: "zero opcode and emitter", input: `{"opcode":0,"emitter":0,"metadata":{},"payload":{}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var input dataJSON
			if err := json.Unmarshal([]byte(tt.input), &input); err != nil {
				t.Errorf("Unmarshal(%q) error = %v, want nil", tt.input, err)
				return
			}

			err := input.validate()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("validate(%s) error = %v, want %v", tt.input, err, tt.wantErr)
			}
			if tt.wantErr == nil {
				return
			}

			got, err := input.toData()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("toData(%s) error = %v, want %v", tt.input, err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, Data{}) {
				t.Errorf("toData(%s) = %#v, want %#v", tt.input, got, Data{})
			}
		})
	}
}

// TestDataJSONToData checks conversion and propagation of nested decoding errors.
func TestDataJSONToData(t *testing.T) {
	tests := []struct {
		name          string
		opcode        op
		emitter       emitter
		metadata      string
		payload       string
		want          Data
		wantErr       error
		wantDecodeErr bool
	}{
		{
			name:     "client message",
			opcode:   produceMessage,
			emitter:  client,
			metadata: `{"clientId":"client-1","clientIPC":"/client-1"}`,
			payload:  `{"raw":false,"message":{"key":"key","value":"value"}}`,
			want: Data{
				opcode:   produceMessage,
				emitter:  client,
				metadata: ClientMetadata{clientId: "client-1", clientIPC: "/client-1"},
				payload:  &ProduceMessagePayload{raw: false, message: Message{key: "key", value: "value"}},
			},
		},
		{
			name:     "server payload",
			opcode:   listTopic,
			emitter:  server,
			metadata: `{}`,
			payload:  `{}`,
			want: Data{
				opcode:   listTopic,
				emitter:  server,
				metadata: ServerMetadata{},
				payload:  &ListTopicPayload{},
			},
		},
		{name: "unknown emitter", emitter: 2, metadata: `{}`, payload: `{}`, wantErr: ErrUnknownEmitter},
		{name: "invalid metadata JSON", emitter: server, metadata: `{`, payload: `{}`, wantDecodeErr: true},
		{name: "unknown metadata field", emitter: server, metadata: `{"extra":true}`, payload: `{}`, wantDecodeErr: true},
		{name: "missing clientId", emitter: client, metadata: `{"clientIPC":"/client"}`, payload: `{}`, wantErr: ErrMissingClientID},
		{name: "missing clientIPC", emitter: client, metadata: `{"clientId":"client"}`, payload: `{}`, wantErr: ErrMissingClientIPC},
		{name: "unknown opcode", opcode: 9, emitter: server, metadata: `{}`, payload: `{}`, wantErr: ErrUnknownOpcode},
		{name: "invalid payload JSON", emitter: server, metadata: `{}`, payload: `{`, wantDecodeErr: true},
		{name: "unknown payload field", emitter: server, metadata: `{}`, payload: `{"extra":true}`, wantDecodeErr: true},
		{name: "missing raw", opcode: produceMessage, emitter: server, metadata: `{}`, payload: `{"message":{"key":"k","value":"v"}}`, wantErr: ErrMissingRaw},
		{name: "missing message", opcode: produceMessage, emitter: server, metadata: `{}`, payload: `{"raw":true}`, wantErr: ErrMissingMessage},
		{name: "invalid message", opcode: produceMessage, emitter: server, metadata: `{}`, payload: `{"raw":true,"message":{"key":"k"}}`, wantErr: ErrMissingMessageValue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := json.RawMessage(tt.metadata)
			payload := json.RawMessage(tt.payload)
			input := dataJSON{Opcode: &tt.opcode, Emitter: &tt.emitter, Metadata: &metadata, Payload: &payload}
			frame := fmt.Sprintf(`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`, tt.opcode, tt.emitter, tt.metadata, tt.payload)

			got, err := input.toData()
			if tt.wantDecodeErr {
				if err == nil {
					t.Errorf("toData(%s) error = nil, want a JSON decoding error", frame)
				}
			} else if !errors.Is(err, tt.wantErr) {
				t.Errorf("toData(%s) error = %v, want %v", frame, err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toData(%s) = %#v, want %#v", frame, got, tt.want)
			}
		})
	}
}
