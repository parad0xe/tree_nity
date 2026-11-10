package protocol

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// TestConsumerInfoPayloadToJSONRejectsInvalidInput checks subscriber names and routing.
func TestConsumerInfoPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *ConsumerInfoPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Client, opcode: ConsumerInfo, wantErr: ErrMissingPayload},
		{name: "zero value", emitter: Client, opcode: ConsumerInfo, payload: &ConsumerInfoPayload{}, wantErr: ErrInvalidPayload},
		{name: "long subscriber", emitter: Client, opcode: ConsumerInfo, payload: &ConsumerInfoPayload{clientID: "client-1", subscriber: strings.Repeat("c", 33)}, wantErr: ErrInvalidPayload},
		{name: "invalid subscriber", emitter: Client, opcode: ConsumerInfo, payload: &ConsumerInfoPayload{clientID: "client-1", subscriber: "consumer/name"}, wantErr: ErrInvalidPayload},
		{name: "invalid UTF-8", emitter: Client, opcode: ConsumerInfo, payload: &ConsumerInfoPayload{clientID: "client-1", subscriber: "\xff"}, wantErr: ErrInvalidPayload},
		{name: "wrong opcode", emitter: Client, opcode: ListTopic, payload: &ConsumerInfoPayload{clientID: "client-1", subscriber: "consumer-1"}, wantErr: ErrInvalidPayload},
		{name: "server emitter", emitter: Server, opcode: ConsumerInfoReply, payload: &ConsumerInfoPayload{clientID: "client-1", subscriber: "consumer-1"}, wantErr: ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.payload.toJSON(tt.emitter, tt.opcode)
			if !errors.Is(err, tt.wantErr) || got != nil {
				t.Errorf("toJSON(emitter=%d, opcode=%d, payload=%#v) = %#v, error = %v, want nil and %v", tt.emitter, tt.opcode, tt.payload, got, err, tt.wantErr)
			}
		})
	}
}

// TestConsumerInfoReplyPayloadToJSONRejectsInvalidInput checks response content and routing.
func TestConsumerInfoReplyPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *ConsumerInfoReplyPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Server, opcode: ConsumerInfoReply, wantErr: ErrMissingPayload},
		{name: "zero value", emitter: Server, opcode: ConsumerInfoReply, payload: &ConsumerInfoReplyPayload{}, wantErr: ErrInvalidPayload},
		{name: "invalid client", emitter: Server, opcode: ConsumerInfoReply, payload: &ConsumerInfoReplyPayload{client: "consumer/name", topic: "orders"}, wantErr: ErrInvalidPayload},
		{name: "invalid topic", emitter: Server, opcode: ConsumerInfoReply, payload: &ConsumerInfoReplyPayload{client: "consumer-1", topic: "user/events"}, wantErr: ErrInvalidPayload},
		{name: "invalid prefix UTF-8", emitter: Server, opcode: ConsumerInfoReply, payload: &ConsumerInfoReplyPayload{client: "consumer-1", topic: "orders", prefix: "\xff"}, wantErr: ErrInvalidPayload},
		{name: "invalid IPC UTF-8", emitter: Server, opcode: ConsumerInfoReply, payload: &ConsumerInfoReplyPayload{client: "consumer-1", topic: "orders", ipc: "\xff"}, wantErr: ErrInvalidPayload},
		{name: "wrong opcode", emitter: Server, opcode: ErrorReply, payload: &ConsumerInfoReplyPayload{client: "consumer-1", topic: "orders"}, wantErr: ErrInvalidPayload},
		{name: "client emitter", emitter: Client, opcode: ConsumerInfo, payload: &ConsumerInfoReplyPayload{client: "consumer-1", topic: "orders"}, wantErr: ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.payload.toJSON(tt.emitter, tt.opcode)
			if !errors.Is(err, tt.wantErr) || got != nil {
				t.Errorf("toJSON(emitter=%d, opcode=%d, payload=%#v) = %#v, error = %v, want nil and %v", tt.emitter, tt.opcode, tt.payload, got, err, tt.wantErr)
			}
		})
	}
}

// TestDecodeRejectsInvalidConsumerInfo checks the required subscriber and strict JSON fields.
func TestDecodeRejectsInvalidConsumerInfo(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "missing subscriber", payload: `{"clientId":"client-1"}`, wantErr: ErrMissingSubscriber},
		{name: "null subscriber", payload: `{"clientId":"client-1","subscriber":null}`, wantErr: ErrMissingSubscriber},
		{name: "empty subscriber", payload: `{"clientId":"client-1","subscriber":""}`, wantErr: ErrInvalidPayload},
		{name: "long subscriber", payload: `{"clientId":"client-1","subscriber":"` + strings.Repeat("c", 33) + `"}`, wantErr: ErrInvalidPayload},
		{name: "large subscriber", payload: `{"clientId":"client-1","subscriber":"` + strings.Repeat("c", 4096) + `"}`, wantErr: ErrInvalidPayload},
		{name: "space in subscriber", payload: `{"clientId":"client-1","subscriber":"consumer name"}`, wantErr: ErrInvalidPayload},
		{name: "slash in subscriber", payload: `{"clientId":"client-1","subscriber":"consumer/name"}`, wantErr: ErrInvalidPayload},
		{name: "non-ASCII subscriber", payload: `{"clientId":"client-1","subscriber":"consommateur-é"}`, wantErr: ErrInvalidPayload},
		{name: "numeric subscriber", payload: `{"clientId":"client-1","subscriber":1}`},
		{name: "boolean subscriber", payload: `{"clientId":"client-1","subscriber":false}`},
		{name: "array subscriber", payload: `{"clientId":"client-1","subscriber":[]}`},
		{name: "object subscriber", payload: `{"clientId":"client-1","subscriber":{}}`},
		{name: "unknown field", payload: `{"clientId":"client-1","subscriber":"consumer-1","extra":true}`},
		{name: "null payload", payload: `null`, wantErr: ErrMissingPayload},
		{name: "array payload", payload: `[]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := `{"opcode":2,"emitter":0,"metadata":{"clientIPC":"/caller-1"},"payload":` + tt.payload + `}`
			got, err := Decode([]byte(frame))
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and an error", frame, got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Decode(%q) error = %v, want %v", frame, err, tt.wantErr)
			}
		})
	}
}

// TestDecodeRejectsMissingConsumerInfoReplyFields checks presence independently of zero or empty values.
func TestDecodeRejectsMissingConsumerInfoReplyFields(t *testing.T) {
	fields := []struct {
		name    string
		wantErr error
	}{
		{name: "client", wantErr: ErrMissingClient},
		{name: "topic", wantErr: ErrMissingTopic},
		{name: "offset", wantErr: ErrMissingOffset},
		{name: "prefix", wantErr: ErrMissingPrefix},
		{name: "ipc", wantErr: ErrMissingIPC},
	}
	states := []struct {
		name string
		null bool
	}{
		{name: "missing"},
		{name: "null", null: true},
	}
	for _, field := range fields {
		for _, state := range states {
			t.Run(field.name+"/"+state.name, func(t *testing.T) {
				payload := map[string]any{"client": "consumer-1", "topic": "orders", "offset": 0, "prefix": "", "ipc": ""}
				if state.null {
					payload[field.name] = nil
				} else {
					delete(payload, field.name)
				}
				body, err := json.Marshal(payload)
				if err != nil {
					t.Errorf("Marshal(%#v) error = %v, want nil", payload, err)
					return
				}
				frame := `{"opcode":2,"emitter":1,"metadata":{},"payload":` + string(body) + `}`
				got, err := Decode([]byte(frame))
				if !errors.Is(err, field.wantErr) || !reflect.DeepEqual(got, Data{}) {
					t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and %v", frame, got, err, field.wantErr)
				}
			})
		}
	}
}

// TestDecodeRejectsInvalidConsumerInfoReplyFields checks field types, ranges, and unknown fields.
func TestDecodeRejectsInvalidConsumerInfoReplyFields(t *testing.T) {
	tests := []struct {
		name          string
		field         string
		value         string
		wantErr       error
		wantTypeError bool
	}{
		{name: "empty client", field: "client", value: `""`, wantErr: ErrInvalidPayload},
		{name: "long client", field: "client", value: `"` + strings.Repeat("c", 33) + `"`, wantErr: ErrInvalidPayload},
		{name: "invalid client", field: "client", value: `"consumer/name"`, wantErr: ErrInvalidPayload},
		{name: "numeric client", field: "client", value: `1`},
		{name: "array client", field: "client", value: `[]`},
		{name: "empty topic", field: "topic", value: `""`, wantErr: ErrInvalidPayload},
		{name: "long topic", field: "topic", value: `"` + strings.Repeat("t", 33) + `"`, wantErr: ErrInvalidPayload},
		{name: "invalid topic", field: "topic", value: `"user/events"`, wantErr: ErrInvalidPayload},
		{name: "non-ASCII topic", field: "topic", value: `"événements"`, wantErr: ErrInvalidPayload},
		{name: "boolean topic", field: "topic", value: `false`},
		{name: "negative offset", field: "offset", value: `-1`, wantTypeError: true},
		{name: "large negative offset", field: "offset", value: `-2147483648`, wantTypeError: true},
		{name: "overflow offset", field: "offset", value: `4294967296`, wantTypeError: true},
		{name: "underflow offset", field: "offset", value: `-2147483649`, wantTypeError: true},
		{name: "fractional offset", field: "offset", value: `0.5`},
		{name: "string offset", field: "offset", value: `"0"`},
		{name: "boolean offset", field: "offset", value: `false`},
		{name: "array offset", field: "offset", value: `[]`},
		{name: "object offset", field: "offset", value: `{}`},
		{name: "numeric prefix", field: "prefix", value: `1`},
		{name: "array prefix", field: "prefix", value: `[]`},
		{name: "object prefix", field: "prefix", value: `{}`},
		{name: "boolean IPC", field: "ipc", value: `false`},
		{name: "array IPC", field: "ipc", value: `[]`},
		{name: "object IPC", field: "ipc", value: `{}`},
		{name: "request field", field: "subscriber", value: `"consumer-1"`},
		{name: "unknown field", field: "extra", value: `true`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := map[string]json.RawMessage{"client": json.RawMessage(`"consumer-1"`), "topic": json.RawMessage(`"orders"`), "offset": json.RawMessage(`0`), "prefix": json.RawMessage(`""`), "ipc": json.RawMessage(`""`)}
			payload[tt.field] = json.RawMessage(tt.value)
			body, err := json.Marshal(payload)
			if err != nil {
				t.Errorf("Marshal(%#v) error = %v, want nil", payload, err)
				return
			}
			frame := `{"opcode":2,"emitter":1,"metadata":{},"payload":` + string(body) + `}`
			got, err := Decode([]byte(frame))
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and an error", frame, got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Decode(%q) error = %v, want %v", frame, err, tt.wantErr)
			}
			if tt.wantTypeError {
				var typeErr *json.UnmarshalTypeError
				if !errors.As(err, &typeErr) || typeErr.Type != reflect.TypeFor[uint32]() {
					t.Errorf("Decode(%q) error = %v, want a JSON type error for uint32", frame, err)
				}
			}
		})
	}
}
