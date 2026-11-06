package protocol

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestServerShutdownPayloadToJSONRejectsInvalidInput checks sentinel routing and nil pointers.
func TestServerShutdownPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *ServerShutdownPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Server, opcode: ServerShutdown, wantErr: ErrMissingPayload},
		{name: "wrong opcode", emitter: Server, opcode: ErrorReply, payload: &ServerShutdownPayload{}, wantErr: ErrInvalidPayload},
		{name: "client emitter", emitter: Client, opcode: ConsumerDisconnect, payload: &ServerShutdownPayload{}, wantErr: ErrInvalidPayload},
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

// TestDecodeRejectsInvalidLifecyclePayloads checks that lifecycle messages require empty JSON objects.
func TestDecodeRejectsInvalidLifecyclePayloads(t *testing.T) {
	operations := []struct {
		name     string
		emitter  Emitter
		opcode   Opcode
		metadata string
	}{
		{name: "disconnect", emitter: Client, opcode: ConsumerDisconnect, metadata: `{"clientId":"client-1","clientIPC":"/client-1"}`},
		{name: "heartbeat", emitter: Client, opcode: ConsumerHeartbeat, metadata: `{"clientId":"client-1","clientIPC":"/client-1"}`},
		{name: "shutdown", emitter: Server, opcode: ServerShutdown, metadata: `{}`},
	}
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "subscriber field", payload: `{"clientId":"client-1"}`},
		{name: "offset field", payload: `{"offset":0}`},
		{name: "reason field", payload: `{"reason":"shutdown"}`},
		{name: "large unknown field", payload: `{"extra":"` + strings.Repeat("x", 4096) + `"}`},
		{name: "null", payload: `null`, wantErr: ErrMissingPayload},
		{name: "array", payload: `[]`},
		{name: "string", payload: `"shutdown"`},
		{name: "number", payload: `1`},
		{name: "boolean", payload: `false`},
	}
	for _, operation := range operations {
		for _, tt := range tests {
			t.Run(operation.name+"/"+tt.name, func(t *testing.T) {
				frame := fmt.Sprintf(`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`, operation.opcode, operation.emitter, operation.metadata, tt.payload)
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
}
