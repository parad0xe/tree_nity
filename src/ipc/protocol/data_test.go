package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// TestDataJSONValidate checks envelopes with every required field present.
func TestDataJSONValidate(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
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

			if err := input.validate(); err != nil {
				t.Errorf("validate(%s) error = %v, want nil", tt.input, err)
			}
		})
	}
}

// TestDataJSONValidateRejectsMissingFields checks rejection before frame conversion.
func TestDataJSONValidateRejectsMissingFields(t *testing.T) {
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

// TestDataJSONToData checks conversion of client frames.
func TestDataJSONToData(t *testing.T) {
	tests := []struct {
		name     string
		opcode   Opcode
		emitter  Emitter
		metadata string
		payload  string
		want     Data
	}{
		{
			name:     "client message",
			opcode:   ProduceMessage,
			emitter:  Client,
			metadata: `{"clientId":"client-1","clientIPC":"/client-1"}`,
			payload:  `{"topic":"orders","message":{"key":"a2V5","value":"dmFsdWU="}}`,
			want: Data{
				opcode:   ProduceMessage,
				emitter:  Client,
				metadata: &ClientMetadata{clientID: "client-1", clientIPC: "/client-1"},
				payload:  &ProduceMessagePayload{topic: "orders", message: Message{key: []byte("key"), value: []byte("value")}},
			},
		},
		{
			name:     "client list topic",
			opcode:   ListTopic,
			emitter:  Client,
			metadata: `{"clientId":"client","clientIPC":""}`,
			payload:  `{}`,
			want: Data{
				opcode:   ListTopic,
				emitter:  Client,
				metadata: &ClientMetadata{clientID: "client"},
				payload:  &ListTopicPayload{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := json.RawMessage(tt.metadata)
			payload := json.RawMessage(tt.payload)
			input := dataJSON{Opcode: &tt.opcode, Emitter: &tt.emitter, Metadata: &metadata, Payload: &payload}
			frame := fmt.Sprintf(`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`, tt.opcode, tt.emitter, tt.metadata, tt.payload)

			got, err := input.toData()
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toData(%s) = %#v, error = %v, want %#v and nil", frame, got, err, tt.want)
			}
		})
	}
}

// TestDecodeRejectsInheritedJSONFields checks that fields cannot leak between decoded frames.
func TestDecodeRejectsInheritedJSONFields(t *testing.T) {
	previousFrame := `{"opcode":6,"emitter":0,"metadata":{"clientId":"previous","clientIPC":"/previous"},"payload":{"topic":"orders","message":{"key":"cHJldmlvdXM=","value":"cHJldmlvdXM="}}}`
	tests := []struct {
		name     string
		metadata string
		payload  string
		wantErr  error
	}{
		{
			name: "missing client ID", metadata: `{"clientIPC":"/next"}`,
			payload: `{"topic":"orders","message":{"key":"bmV4dA==","value":"bmV4dA=="}}`, wantErr: ErrMissingClientID,
		},
		{
			name: "missing topic", metadata: `{"clientId":"next","clientIPC":"/next"}`,
			payload: `{"message":{"key":"bmV4dA==","value":"bmV4dA=="}}`, wantErr: ErrMissingTopic,
		},
		{
			name: "missing message value", metadata: `{"clientId":"next","clientIPC":"/next"}`,
			payload: `{"topic":"orders","message":{"key":"bmV4dA=="}}`, wantErr: ErrMissingMessageValue,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Decode([]byte(previousFrame)); err != nil {
				t.Errorf("Decode(%q) error = %v, want nil", previousFrame, err)
				return
			}
			frame := fmt.Sprintf(`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`, ProduceMessage, Client, tt.metadata, tt.payload)
			got, err := Decode([]byte(frame))
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) after Decode(%q) = %#v, error = %v, want zero Data and %v", frame, previousFrame, got, err, tt.wantErr)
			}
		})
	}
}

// TestDataJSONToDataRejectsInvalidInput checks propagation of nested decoding errors.
func TestDataJSONToDataRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		opcode   Opcode
		emitter  Emitter
		metadata string
		payload  string
		wantErr  error
	}{
		{name: "unknown emitter", emitter: 2, metadata: `{}`, payload: `{}`, wantErr: ErrUnknownEmitter},
		{name: "invalid metadata JSON", emitter: Server, metadata: `{`, payload: `{}`},
		{name: "unknown metadata field", emitter: Server, metadata: `{"extra":true}`, payload: `{}`},
		{name: "missing clientId", emitter: Client, metadata: `{"clientIPC":"/client"}`, payload: `{}`, wantErr: ErrMissingClientID},
		{name: "missing clientIPC", emitter: Client, metadata: `{"clientId":"client"}`, payload: `{}`, wantErr: ErrMissingClientIPC},
		{name: "unknown opcode", opcode: 9, emitter: Client, metadata: `{"clientId":"client","clientIPC":""}`, payload: `{}`, wantErr: ErrUnknownOpcode},
		{name: "invalid payload JSON", emitter: Client, metadata: `{"clientId":"client","clientIPC":""}`, payload: `{`},
		{name: "unknown payload field", emitter: Client, metadata: `{"clientId":"client","clientIPC":""}`, payload: `{"extra":true}`},
		{name: "missing topic", opcode: ProduceMessage, emitter: Client, metadata: `{"clientId":"client","clientIPC":""}`, payload: `{"message":{"key":"aw==","value":"dg=="}}`, wantErr: ErrMissingTopic},
		{name: "missing message", opcode: ProduceMessage, emitter: Client, metadata: `{"clientId":"client","clientIPC":""}`, payload: `{"topic":"orders"}`, wantErr: ErrMissingMessage},
		{name: "invalid message", opcode: ProduceMessage, emitter: Client, metadata: `{"clientId":"client","clientIPC":""}`, payload: `{"topic":"orders","message":{"key":"aw=="}}`, wantErr: ErrMissingMessageValue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := json.RawMessage(tt.metadata)
			payload := json.RawMessage(tt.payload)
			input := dataJSON{Opcode: &tt.opcode, Emitter: &tt.emitter, Metadata: &metadata, Payload: &payload}
			frame := fmt.Sprintf(`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`, tt.opcode, tt.emitter, tt.metadata, tt.payload)

			got, err := input.toData()
			if err == nil {
				t.Errorf("toData(%s) error = nil, want an error", frame)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("toData(%s) error = %v, want %v", frame, err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, Data{}) {
				t.Errorf("toData(%s) = %#v, want zero Data", frame, got)
			}
		})
	}
}
