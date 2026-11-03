package protocol

import (
	"errors"
	"reflect"
	"testing"
)

// TestServerOpcodeValues fixes the emitter-specific operation numbers on the wire.
func TestServerOpcodeValues(t *testing.T) {
	tests := []struct {
		name   string
		opcode Opcode
		want   int
	}{
		{name: "list topic reply", opcode: ListTopicReply, want: 0},
		{name: "create topic reply", opcode: CreateTopicReply, want: 1},
		{name: "consumer info reply", opcode: ConsumerInfoReply, want: 2},
		{name: "topic subscription reply", opcode: TopicSubscriptionReply, want: 3},
		{name: "check topic reply", opcode: CheckTopicReply, want: 4},
		{name: "error reply", opcode: ErrorReply, want: 5},
		{name: "message delivery", opcode: MessageDelivery, want: 6},
		{name: "server shutdown", opcode: ServerShutdown, want: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := int(tt.opcode); got != tt.want {
				t.Errorf("int(%s) = %d, want %d", tt.name, got, tt.want)
			}
		})
	}
}

// TestOpcodeValidateRejectsInvalidServerOperations checks unknown codes and emitters.
func TestOpcodeValidateRejectsInvalidServerOperations(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		wantErr error
	}{
		{name: "negative opcode", emitter: Server, opcode: -1, wantErr: ErrUnknownOpcode},
		{name: "first unknown opcode", emitter: Server, opcode: 8, wantErr: ErrUnknownOpcode},
		{name: "unknown opcode", emitter: Server, opcode: 256, wantErr: ErrUnknownOpcode},
		{name: "negative emitter", emitter: -1, opcode: ErrorReply, wantErr: ErrUnknownEmitter},
		{name: "unknown emitter", emitter: 2, opcode: ErrorReply, wantErr: ErrUnknownEmitter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.opcode.validate(tt.emitter); !errors.Is(err, tt.wantErr) {
				t.Errorf("Opcode(%d).validate(%d) error = %v, want %v", tt.opcode, tt.emitter, err, tt.wantErr)
			}
		})
	}
}

// TestErrorReplyPayloadToJSONRejectsInvalidInput checks business and routing validation.
func TestErrorReplyPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *ErrorReplyPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Server, opcode: ErrorReply, wantErr: ErrMissingPayload},
		{name: "negative kind", emitter: Server, opcode: ErrorReply, payload: &ErrorReplyPayload{kind: -1}, wantErr: ErrInvalidPayload},
		{name: "unknown kind", emitter: Server, opcode: ErrorReply, payload: &ErrorReplyPayload{kind: 5}, wantErr: ErrInvalidPayload},
		{name: "invalid UTF-8 reason", emitter: Server, opcode: ErrorReply, payload: &ErrorReplyPayload{reason: "\xff"}, wantErr: ErrInvalidPayload},
		{name: "wrong opcode", emitter: Server, opcode: ListTopicReply, payload: &ErrorReplyPayload{}, wantErr: ErrInvalidPayload},
		{name: "client emitter", emitter: Client, opcode: CheckTopic, payload: &ErrorReplyPayload{}, wantErr: ErrInvalidPayload},
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

// TestDecodeRejectsInvalidErrorReply checks strict JSON decoding and required error fields.
func TestDecodeRejectsInvalidErrorReply(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "empty object", payload: `{}`, wantErr: ErrMissingErrorKind},
		{name: "missing kind", payload: `{"reason":"error"}`, wantErr: ErrMissingErrorKind},
		{name: "null kind", payload: `{"kind":null,"reason":"error"}`, wantErr: ErrMissingErrorKind},
		{name: "missing reason", payload: `{"kind":0}`, wantErr: ErrMissingErrorReason},
		{name: "null reason", payload: `{"kind":0,"reason":null}`, wantErr: ErrMissingErrorReason},
		{name: "negative kind", payload: `{"kind":-1,"reason":"error"}`, wantErr: ErrInvalidPayload},
		{name: "unknown kind", payload: `{"kind":5,"reason":"error"}`, wantErr: ErrInvalidPayload},
		{name: "string kind", payload: `{"kind":"0","reason":"error"}`},
		{name: "fractional kind", payload: `{"kind":0.5,"reason":"error"}`},
		{name: "boolean kind", payload: `{"kind":false,"reason":"error"}`},
		{name: "array kind", payload: `{"kind":[],"reason":"error"}`},
		{name: "numeric reason", payload: `{"kind":0,"reason":1}`},
		{name: "boolean reason", payload: `{"kind":0,"reason":false}`},
		{name: "array reason", payload: `{"kind":0,"reason":[]}`},
		{name: "unknown field", payload: `{"kind":0,"reason":"error","extra":true}`},
		{name: "null payload", payload: `null`, wantErr: ErrMissingPayload},
		{name: "array payload", payload: `[]`},
		{name: "string payload", payload: `"error"`},
		{name: "number payload", payload: `1`},
		{name: "boolean payload", payload: `false`},
		{name: "malformed payload", payload: `{`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := `{"opcode":5,"emitter":1,"metadata":{},"payload":` + tt.payload + `}`
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

// TestDecodeRejectsErrorReplyFromClient checks the opcode shared with CheckTopic.
func TestDecodeRejectsErrorReplyFromClient(t *testing.T) {
	frame := `{"opcode":5,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"kind":0,"reason":"error"}}`
	got, err := Decode([]byte(frame))
	if err == nil || !reflect.DeepEqual(got, Data{}) {
		t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and an unknown field error", frame, got, err)
	}
}

// TestCreateTopicReplyPayloadToJSONRejectsInvalidInput checks response routing and nil pointers.
func TestCreateTopicReplyPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *CreateTopicReplyPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Server, opcode: CreateTopicReply, wantErr: ErrMissingPayload},
		{name: "wrong opcode", emitter: Server, opcode: ErrorReply, payload: &CreateTopicReplyPayload{}, wantErr: ErrInvalidPayload},
		{name: "client emitter", emitter: Client, opcode: CreateTopic, payload: &CreateTopicReplyPayload{}, wantErr: ErrInvalidPayload},
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

// TestDecodeRejectsInvalidCreateTopicReply checks that the success response is an empty object.
func TestDecodeRejectsInvalidCreateTopicReply(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "topic field", payload: `{"topic":"orders"}`},
		{name: "unknown field", payload: `{"extra":true}`},
		{name: "array", payload: `[]`},
		{name: "string", payload: `"created"`},
		{name: "number", payload: `1`},
		{name: "boolean", payload: `false`},
		{name: "null", payload: `null`, wantErr: ErrMissingPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := `{"opcode":1,"emitter":1,"metadata":{},"payload":` + tt.payload + `}`
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
