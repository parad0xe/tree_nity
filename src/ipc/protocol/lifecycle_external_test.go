package protocol_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"tree_nity/src/ipc/protocol"
)

// TestLifecyclePayloadRoundTrip checks client IDs and emitter-specific decoding of opcode seven.
func TestLifecyclePayloadRoundTrip(t *testing.T) {
	client := protocol.NewClientMetadata("/client-1")
	server := protocol.NewServerMetadata()
	tests := []struct {
		name     string
		opcode   protocol.Opcode
		emitter  protocol.Emitter
		metadata protocol.Metadata
		json     string
		body     string
		payload  protocol.Payload
	}{
		{name: "disconnect constructor", opcode: protocol.ConsumerDisconnect, emitter: protocol.Client, metadata: client, json: `{"clientIPC":"/client-1"}`, body: `{"clientId":"client-1"}`, payload: protocol.NewConsumerDisconnectPayload("client-1")},
		{name: "heartbeat constructor", opcode: protocol.ConsumerHeartbeat, emitter: protocol.Client, metadata: client, json: `{"clientIPC":"/client-1"}`, body: `{"clientId":"client-1"}`, payload: protocol.NewConsumerHeartbeatPayload("client-1")},
		{name: "shutdown constructor", opcode: protocol.ServerShutdown, emitter: protocol.Server, metadata: server, json: `{}`, body: `{}`, payload: protocol.NewServerShutdownPayload()},
		{name: "shutdown zero value", opcode: protocol.ServerShutdown, emitter: protocol.Server, metadata: server, json: `{}`, body: `{}`, payload: &protocol.ServerShutdownPayload{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := protocol.NewData(tt.opcode, tt.metadata, tt.payload)
			if err != nil {
				t.Errorf("NewData(%d, %#v, %#v) error = %v, want nil", tt.opcode, tt.metadata, tt.payload, err)
				return
			}
			frame, err := protocol.Encode(data)
			wantFrame := fmt.Sprintf(`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`, tt.opcode, tt.emitter, tt.json, tt.body)
			if err != nil || string(frame) != wantFrame {
				t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
				return
			}
			decoded, err := protocol.Decode(frame)
			if err != nil || !reflect.DeepEqual(decoded, data) {
				t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, decoded, err, data)
			}
		})
	}
}

// TestNewDataRejectsInvalidServerShutdown checks nil sentinels and incompatible routing.
func TestNewDataRejectsInvalidServerShutdown(t *testing.T) {
	tests := []struct {
		name     string
		opcode   protocol.Opcode
		metadata protocol.Metadata
		payload  protocol.Payload
		wantErr  error
	}{
		{name: "nil payload", opcode: protocol.ServerShutdown, metadata: protocol.NewServerMetadata(), wantErr: protocol.ErrMissingPayload},
		{name: "nil payload pointer", opcode: protocol.ServerShutdown, metadata: protocol.NewServerMetadata(), payload: (*protocol.ServerShutdownPayload)(nil), wantErr: protocol.ErrMissingPayload},
		{name: "wrong opcode", opcode: protocol.ErrorReply, metadata: protocol.NewServerMetadata(), payload: protocol.NewServerShutdownPayload(), wantErr: protocol.ErrInvalidPayload},
		{name: "client metadata", opcode: protocol.ServerShutdown, metadata: protocol.NewClientMetadata("/client-1"), payload: protocol.NewServerShutdownPayload(), wantErr: protocol.ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protocol.NewData(tt.opcode, tt.metadata, tt.payload)
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(got, protocol.Data{}) {
				t.Errorf("NewData(%d, %#v, %#v) = %#v, error = %v, want zero Data and %v", tt.opcode, tt.metadata, tt.payload, got, err, tt.wantErr)
			}
		})
	}
}
