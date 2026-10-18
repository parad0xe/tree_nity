package protocol

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

// TestDataJSONValidate checks that every envelope field must be present and non-null.
func TestDataJSONValidate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "empty envelope", input: `{}`, wantErr: "missing opcode"},
		{name: "missing opcode", input: `{"emitter":1,"metadata":{},"payload":{}}`, wantErr: "missing opcode"},
		{name: "null opcode", input: `{"opcode":null,"emitter":1,"metadata":{},"payload":{}}`, wantErr: "missing opcode"},
		{name: "missing emitter", input: `{"opcode":0,"metadata":{},"payload":{}}`, wantErr: "missing emitter"},
		{name: "null emitter", input: `{"opcode":0,"emitter":null,"metadata":{},"payload":{}}`, wantErr: "missing emitter"},
		{name: "missing metadata", input: `{"opcode":0,"emitter":1,"payload":{}}`, wantErr: "missing metadata"},
		{name: "null metadata", input: `{"opcode":0,"emitter":1,"metadata":null,"payload":{}}`, wantErr: "missing metadata"},
		{name: "missing payload", input: `{"opcode":0,"emitter":1,"metadata":{}}`, wantErr: "missing payload"},
		{name: "null payload", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":null}`, wantErr: "missing payload"},
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
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("validate(%s) error = %v, want nil", tt.input, err)
				}
				return
			}
			if err == nil || err.Error() != tt.wantErr {
				t.Errorf("validate(%s) error = %v, want %q", tt.input, err, tt.wantErr)
			}

			got, err := input.toData()
			if err == nil || err.Error() != tt.wantErr {
				t.Errorf("toData(%s) error = %v, want %q", tt.input, err, tt.wantErr)
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
		name     string
		opcode   op
		emitter  emitter
		metadata string
		payload  string
		want     Data
		wantErr  bool
	}{
		{
			name:     "client message",
			opcode:   produce_message,
			emitter:  client,
			metadata: `{"clientId":"client-1","clientIPC":"/client-1"}`,
			payload:  `{"raw":false,"message":{"key":"key","value":"value"}}`,
			want: Data{
				opcode:   produce_message,
				emitter:  client,
				metadata: ClientMetadata{clientId: "client-1", clientIPC: "/client-1"},
				payload:  &ProduceMessagePayload{raw: false, message: Message{key: "key", value: "value"}},
			},
		},
		{
			name:     "server payload",
			opcode:   list_topic,
			emitter:  server,
			metadata: `{}`,
			payload:  `{}`,
			want: Data{
				opcode:   list_topic,
				emitter:  server,
				metadata: ServerMetadata{},
				payload:  &ListTopicPayload{},
			},
		},
		{name: "unknown emitter", emitter: 2, metadata: `{}`, payload: `{}`, wantErr: true},
		{name: "invalid metadata JSON", emitter: server, metadata: `{`, payload: `{}`, wantErr: true},
		{name: "unknown metadata field", emitter: server, metadata: `{"extra":true}`, payload: `{}`, wantErr: true},
		{name: "missing clientId", emitter: client, metadata: `{"clientIPC":"/client"}`, payload: `{}`, wantErr: true},
		{name: "missing clientIPC", emitter: client, metadata: `{"clientId":"client"}`, payload: `{}`, wantErr: true},
		{name: "unknown opcode", opcode: 9, emitter: server, metadata: `{}`, payload: `{}`, wantErr: true},
		{name: "invalid payload JSON", emitter: server, metadata: `{}`, payload: `{`, wantErr: true},
		{name: "unknown payload field", emitter: server, metadata: `{}`, payload: `{"extra":true}`, wantErr: true},
		{name: "missing raw", opcode: produce_message, emitter: server, metadata: `{}`, payload: `{"message":{"key":"k","value":"v"}}`, wantErr: true},
		{name: "missing message", opcode: produce_message, emitter: server, metadata: `{}`, payload: `{"raw":true}`, wantErr: true},
		{name: "invalid message", opcode: produce_message, emitter: server, metadata: `{}`, payload: `{"raw":true,"message":{"key":"k"}}`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := json.RawMessage(tt.metadata)
			payload := json.RawMessage(tt.payload)
			input := dataJSON{Opcode: &tt.opcode, Emitter: &tt.emitter, Metadata: &metadata, Payload: &payload}
			frame := fmt.Sprintf(`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`, tt.opcode, tt.emitter, tt.metadata, tt.payload)

			got, err := input.toData()
			if (err != nil) != tt.wantErr {
				t.Errorf("toData(%s) error = %v, want error = %t", frame, err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toData(%s) = %#v, want %#v", frame, got, tt.want)
			}
		})
	}
}
