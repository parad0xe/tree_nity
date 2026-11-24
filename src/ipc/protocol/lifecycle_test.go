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

// TestDecodeRejectsInvalidLifecyclePayloads checks lifecycle payload types and unexpected fields.
func TestDecodeRejectsInvalidLifecyclePayloads(t *testing.T) {
	operations := []struct {
		name     string
		emitter  Emitter
		opcode   Opcode
		metadata string
	}{
		{name: "disconnect", emitter: Client, opcode: ConsumerDisconnect, metadata: `{"clientIPC":"/client-1"}`},
		{name: "server disconnect", emitter: Server, opcode: ServerConsumerDisconnect, metadata: `{}`},
		{name: "heartbeat", emitter: Client, opcode: ConsumerHeartbeat, metadata: `{"clientIPC":"/client-1"}`},
		{name: "shutdown", emitter: Server, opcode: ServerShutdown, metadata: `{}`},
		{name: "heartbeat reply", emitter: Server, opcode: ConsumerHeartbeatReply, metadata: `{}`},
	}
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "subscriber field", payload: `{"subscriber":"client-1"}`},
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

// TestHeartbeatPayloadToJSONRejectsInvalidInput checks heartbeat routing and nil pointers.
func TestHeartbeatPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *ConsumerHeartbeatPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Client, opcode: ConsumerHeartbeat, wantErr: ErrMissingPayload},
		{name: "wrong opcode", emitter: Client, opcode: ConsumerDisconnect, payload: NewConsumerHeartbeatPayload("client-1"), wantErr: ErrInvalidPayload},
		{name: "server emitter", emitter: Server, opcode: ConsumerHeartbeat, payload: NewConsumerHeartbeatPayload("client-1"), wantErr: ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.payload.toJSON(tt.emitter, tt.opcode)
			if !errors.Is(err, tt.wantErr) || got != nil {
				t.Errorf("heartbeat.toJSON(%d, %d) = %#v, %v, want nil and %v", tt.emitter, tt.opcode, got, err, tt.wantErr)
			}
		})
	}
}

// TestServerDisconnectPayloadToJSONRejectsInvalidInput checks notification validation.
func TestServerDisconnectPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		opcode  Opcode
		payload *ServerConsumerDisconnectPayload
		wantErr error
	}{
		{name: "nil payload", opcode: ServerConsumerDisconnect, wantErr: ErrMissingPayload},
		{name: "missing client ID", opcode: ServerConsumerDisconnect, payload: NewServerConsumerDisconnectPayload(""), wantErr: ErrInvalidPayload},
		{name: "invalid client ID", opcode: ServerConsumerDisconnect, payload: NewServerConsumerDisconnectPayload("client/name"), wantErr: ErrInvalidPayload},
		{name: "shutdown opcode", opcode: ServerShutdown, payload: NewServerConsumerDisconnectPayload("client-1"), wantErr: ErrInvalidPayload},
		{name: "unrelated opcode", opcode: ErrorReply, payload: NewServerConsumerDisconnectPayload("client-1"), wantErr: ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.payload.toJSON(Server, tt.opcode)
			if !errors.Is(err, tt.wantErr) || got != nil {
				t.Errorf("disconnect.toJSON(Server, %d) = %#v, %v, want nil and %v", tt.opcode, got, err, tt.wantErr)
			}
		})
	}
}

// TestDecodeRejectsInvalidLifecycleFields checks required lifecycle identifiers.
func TestDecodeRejectsInvalidLifecycleFields(t *testing.T) {
	tests := []struct {
		name     string
		emitter  Emitter
		opcode   Opcode
		metadata string
		payload  string
		wantErr  error
	}{
		{name: "heartbeat missing ID", emitter: Client, opcode: ConsumerHeartbeat, metadata: `{"clientIPC":"/client-1"}`, payload: `{}`, wantErr: ErrMissingClientID},
		{name: "heartbeat consumer ID", emitter: Client, opcode: ConsumerHeartbeat, metadata: `{"clientIPC":"/client-1"}`, payload: `{"consumerId":"client-1"}`, wantErr: ErrUnknownField},
		{name: "disconnect missing ID", emitter: Server, opcode: ServerConsumerDisconnect, metadata: `{}`, payload: `{}`, wantErr: ErrMissingClientID},
		{name: "disconnect null ID", emitter: Server, opcode: ServerConsumerDisconnect, metadata: `{}`, payload: `{"clientId":null}`, wantErr: ErrMissingClientID},
		{name: "disconnect invalid ID", emitter: Server, opcode: ServerConsumerDisconnect, metadata: `{}`, payload: `{"clientId":"client/name"}`, wantErr: ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := fmt.Sprintf(`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`, tt.opcode, tt.emitter, tt.metadata, tt.payload)
			got, err := Decode([]byte(frame))
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, %v, want zero Data and %v", frame, got, err, tt.wantErr)
			}
		})
	}
}

// TestHeartbeatReplyPayloadToJSONRejectsInvalidInput checks server-only heartbeat replies.
func TestHeartbeatReplyPayloadToJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *ConsumerHeartbeatReplyPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Server, opcode: ConsumerHeartbeatReply, wantErr: ErrMissingPayload},
		{name: "wrong opcode", emitter: Server, opcode: ServerShutdown, payload: NewConsumerHeartbeatReplyPayload(), wantErr: ErrInvalidPayload},
		{name: "client emitter", emitter: Client, opcode: ConsumerHeartbeat, payload: NewConsumerHeartbeatReplyPayload(), wantErr: ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.payload.toJSON(tt.emitter, tt.opcode)
			if !errors.Is(err, tt.wantErr) || got != nil {
				t.Errorf("heartbeatReply.toJSON(%d, %d) = %#v, %v, want nil and %v", tt.emitter, tt.opcode, got, err, tt.wantErr)
			}
		})
	}
}
