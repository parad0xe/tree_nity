package protocol

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// TestMessageDeliveryPayloadToJSONRejectsInvalidInput checks content and routing validation.
func TestMessageDeliveryPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *MessageDeliveryPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Server, opcode: MessageDelivery, wantErr: ErrMissingPayload},
		{name: "oversized message", emitter: Server, opcode: MessageDelivery, payload: &MessageDeliveryPayload{message: Message{value: []byte(strings.Repeat("v", MessageSize+1))}}, wantErr: ErrMessageTooLarge},
		{name: "wrong opcode", emitter: Server, opcode: ErrorReply, payload: &MessageDeliveryPayload{}, wantErr: ErrInvalidPayload},
		{name: "client emitter", emitter: Client, opcode: ProduceMessage, payload: &MessageDeliveryPayload{}, wantErr: ErrInvalidPayload},
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

// TestDecodeRejectsInvalidMessageDelivery checks required fields, decoded sizes, and uint32 bounds.
func TestDecodeRejectsInvalidMessageDelivery(t *testing.T) {
	oversized := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("v", MessageSize)))
	tests := []struct {
		name          string
		payload       string
		wantErr       error
		wantTypeError bool
	}{
		{name: "missing message", payload: `{}`, wantErr: ErrMissingMessage},
		{name: "null message", payload: `{"message":null}`, wantErr: ErrMissingMessage},
		{name: "missing offset", payload: `{"message":{"key":"","value":""}}`, wantErr: ErrMissingMessageOffset},
		{name: "null offset", payload: `{"message":{"key":"","value":"","offset":null}}`, wantErr: ErrMissingMessageOffset},
		{name: "missing key", payload: `{"message":{"value":"","offset":0}}`, wantErr: ErrMissingMessageKey},
		{name: "null key", payload: `{"message":{"key":null,"value":"","offset":0}}`, wantErr: ErrMissingMessageKey},
		{name: "missing value", payload: `{"message":{"key":"","offset":0}}`, wantErr: ErrMissingMessageValue},
		{name: "null value", payload: `{"message":{"key":"","value":null,"offset":0}}`, wantErr: ErrMissingMessageValue},
		{name: "negative offset", payload: `{"message":{"key":"","value":"","offset":-1}}`, wantTypeError: true},
		{name: "large negative offset", payload: `{"message":{"key":"","value":"","offset":-2147483648}}`, wantTypeError: true},
		{name: "overflow offset", payload: `{"message":{"key":"","value":"","offset":4294967296}}`, wantTypeError: true},
		{name: "underflow offset", payload: `{"message":{"key":"","value":"","offset":-2147483649}}`, wantTypeError: true},
		{name: "fractional offset", payload: `{"message":{"key":"","value":"","offset":0.5}}`},
		{name: "string offset", payload: `{"message":{"key":"","value":"","offset":"0"}}`},
		{name: "boolean offset", payload: `{"message":{"key":"","value":"","offset":false}}`},
		{name: "array offset", payload: `{"message":{"key":"","value":"","offset":[]}}`},
		{name: "object offset", payload: `{"message":{"key":"","value":"","offset":{}}`},
		{name: "invalid base64 key", payload: `{"message":{"key":"?","value":"","offset":0}}`, wantErr: ErrInvalidPayload},
		{name: "invalid base64 value", payload: `{"message":{"key":"","value":"?","offset":0}}`, wantErr: ErrInvalidPayload},
		{name: "numeric key", payload: `{"message":{"key":1,"value":"","offset":0}}`},
		{name: "array key", payload: `{"message":{"key":[0,255],"value":"","offset":0}}`},
		{name: "boolean value", payload: `{"message":{"key":"","value":false,"offset":0}}`},
		{name: "array value", payload: `{"message":{"key":"","value":[0,255],"offset":0}}`},
		{name: "oversized message", payload: `{"message":{"key":"aw==","value":"` + oversized + `","offset":0}}`, wantErr: ErrMessageTooLarge},
		{name: "unknown message field", payload: `{"message":{"key":"","value":"","offset":0,"extra":true}}`},
		{name: "offset outside message", payload: `{"message":{"key":"","value":""},"offset":0}`},
		{name: "unknown payload field", payload: `{"message":{"key":"","value":"","offset":0},"extra":true}`},
		{name: "array message", payload: `{"message":[]}`},
		{name: "null payload", payload: `null`, wantErr: ErrMissingPayload},
		{name: "array payload", payload: `[]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := `{"opcode":6,"emitter":1,"metadata":{},"payload":` + tt.payload + `}`
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

// TestDecodeRejectsProduceMessageOffset checks that delivery fields remain forbidden in production.
func TestDecodeRejectsProduceMessageOffset(t *testing.T) {
	tests := []struct {
		name   string
		offset string
	}{
		{name: "zero", offset: "0"},
		{name: "nonzero", offset: "42"},
		{name: "null", offset: "null"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"key":"","value":"","offset":` + tt.offset + `}}}`
			got, err := Decode([]byte(frame))
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and an unknown field error", frame, got, err)
			}
		})
	}
}
