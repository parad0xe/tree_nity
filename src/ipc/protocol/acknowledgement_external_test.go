package protocol_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"tree_nity/src/ipc/protocol"
)

// TestAcknowledgementRoundTrip checks that the public API preserves the supplied next offset.
func TestAcknowledgementRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		offset uint32
	}{
		{name: "zero", offset: 0},
		{name: "after first message", offset: 1},
		{name: "next offset", offset: 43},
		{name: "maximum signed offset", offset: 2147483647},
		{name: "above signed limit", offset: 2147483648},
		{name: "maximum uint32", offset: 4294967295},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := protocol.NewAcknowledgementPayload(tt.offset)
			data, err := protocol.NewData(protocol.Acknowledgement, protocol.NewClientMetadata("client-1", "/client-1"), payload)
			if err != nil {
				t.Errorf("NewData(Acknowledgement, client, offset=%d) error = %v, want nil", tt.offset, err)
				return
			}
			frame, err := protocol.Encode(data)
			wantFrame := fmt.Sprintf(`{"opcode":4,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"offset":%d}}`, tt.offset)
			if err != nil || string(frame) != wantFrame {
				t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
				return
			}
			decoded, err := protocol.Decode(frame)
			if err != nil || !reflect.DeepEqual(decoded, data) {
				t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, decoded, err, data)
			}
			got, ok := decoded.Payload().(*protocol.AcknowledgementPayload)
			if !ok {
				t.Errorf("Decode(%q).Payload() type = %T, want *AcknowledgementPayload", frame, decoded.Payload())
				return
			}
			if got.Offset() != tt.offset {
				t.Errorf("Decode(%q).Offset() = %d, want %d", frame, got.Offset(), tt.offset)
			}
		})
	}
}

// TestAcknowledgementZeroValue checks that zero is encoded as an explicit offset.
func TestAcknowledgementZeroValue(t *testing.T) {
	payload := &protocol.AcknowledgementPayload{}
	data, err := protocol.NewData(protocol.Acknowledgement, protocol.NewClientMetadata("client-1", "/client-1"), payload)
	if err != nil {
		t.Errorf("NewData(Acknowledgement, client, %#v) error = %v, want nil", payload, err)
		return
	}
	frame, err := protocol.Encode(data)
	wantFrame := `{"opcode":4,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"offset":0}}`
	if err != nil || string(frame) != wantFrame {
		t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
	}
}

// TestNewDataRejectsInvalidAcknowledgement checks public validation and emitter inference.
func TestNewDataRejectsInvalidAcknowledgement(t *testing.T) {
	tests := []struct {
		name     string
		metadata protocol.Metadata
		payload  protocol.Payload
		wantErr  error
	}{
		{name: "nil payload", metadata: protocol.NewClientMetadata("client-1", "/client-1"), wantErr: protocol.ErrMissingPayload},
		{name: "nil payload pointer", metadata: protocol.NewClientMetadata("client-1", "/client-1"), payload: (*protocol.AcknowledgementPayload)(nil), wantErr: protocol.ErrMissingPayload},
		{name: "server metadata", metadata: protocol.NewServerMetadata(), payload: protocol.NewAcknowledgementPayload(43), wantErr: protocol.ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protocol.NewData(protocol.Acknowledgement, tt.metadata, tt.payload)
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(got, protocol.Data{}) {
				t.Errorf("NewData(Acknowledgement, %#v, %#v) = %#v, error = %v, want zero Data and %v", tt.metadata, tt.payload, got, err, tt.wantErr)
			}
		})
	}
}
