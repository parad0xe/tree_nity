package protocol

import (
	"errors"
	"reflect"
	"testing"
)

// TestAcknowledgementPayloadToJSONRejectsInvalidInput checks offset and routing validation.
func TestAcknowledgementPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *AcknowledgementPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Client, opcode: Acknowledgement, wantErr: ErrMissingPayload},
		{name: "negative offset", emitter: Client, opcode: Acknowledgement, payload: &AcknowledgementPayload{offset: -1}, wantErr: ErrInvalidPayload},
		{name: "minimum int32 offset", emitter: Client, opcode: Acknowledgement, payload: &AcknowledgementPayload{offset: -2147483648}, wantErr: ErrInvalidPayload},
		{name: "wrong opcode", emitter: Client, opcode: ListTopic, payload: &AcknowledgementPayload{}, wantErr: ErrInvalidPayload},
		{name: "server emitter", emitter: Server, opcode: CheckTopicReply, payload: &AcknowledgementPayload{}, wantErr: ErrInvalidPayload},
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

// TestDecodeRejectsInvalidAcknowledgement checks the mandatory int32 offset and strict JSON fields.
func TestDecodeRejectsInvalidAcknowledgement(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "missing offset", payload: `{}`, wantErr: ErrMissingOffset},
		{name: "null offset", payload: `{"offset":null}`, wantErr: ErrMissingOffset},
		{name: "negative offset", payload: `{"offset":-1}`, wantErr: ErrInvalidPayload},
		{name: "minimum int32 offset", payload: `{"offset":-2147483648}`, wantErr: ErrInvalidPayload},
		{name: "overflow offset", payload: `{"offset":2147483648}`},
		{name: "underflow offset", payload: `{"offset":-2147483649}`},
		{name: "fractional offset", payload: `{"offset":0.5}`},
		{name: "string offset", payload: `{"offset":"0"}`},
		{name: "boolean offset", payload: `{"offset":false}`},
		{name: "array offset", payload: `{"offset":[]}`},
		{name: "object offset", payload: `{"offset":{}}`},
		{name: "unknown field", payload: `{"offset":0,"extra":true}`},
		{name: "nested offset", payload: `{"message":{"offset":1}}`},
		{name: "null payload", payload: `null`, wantErr: ErrMissingPayload},
		{name: "array payload", payload: `[]`},
		{name: "string payload", payload: `"ack"`},
		{name: "number payload", payload: `1`},
		{name: "boolean payload", payload: `false`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := `{"opcode":4,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":` + tt.payload + `}`
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
