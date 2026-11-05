package protocol_test

import (
	"bytes"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"testing"

	"tree_nity/src/ipc/protocol"
)

// TestMessageDeliveryRoundTrip checks binary content and explicit offsets through the public API.
func TestMessageDeliveryRoundTrip(t *testing.T) {
	maximumValue := []byte(strings.Repeat("v", protocol.MessageSize))
	tests := []struct {
		name   string
		offset uint32
		key    []byte
		value  []byte
		json   string
	}{
		{name: "empty message at zero", json: `{"message":{"key":"","value":"","offset":0}}`},
		{name: "minimal message", offset: 1, key: []byte("k"), value: []byte("v"), json: `{"message":{"key":"aw==","value":"dg==","offset":1}}`},
		{name: "binary bytes", offset: 42, key: []byte("user"), value: []byte{0x00, 0xff, 0x10}, json: `{"message":{"key":"dXNlcg==","value":"AP8Q","offset":42}}`},
		{name: "maximum signed offset", offset: 2147483647, json: `{"message":{"key":"","value":"","offset":2147483647}}`},
		{name: "above signed limit", offset: 2147483648, json: `{"message":{"key":"","value":"","offset":2147483648}}`},
		{name: "maximum offset", offset: 4294967295, json: `{"message":{"key":"","value":"","offset":4294967295}}`},
		{name: "decoded byte limit", offset: 42, value: maximumValue, json: `{"message":{"key":"","value":"` + base64.StdEncoding.EncodeToString(maximumValue) + `","offset":42}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := protocol.NewMessage(tt.key, tt.value)
			payload := protocol.NewMessageDeliveryPayload(tt.offset, message)
			data, err := protocol.NewData(protocol.MessageDelivery, protocol.NewServerMetadata(), payload)
			if err != nil {
				t.Errorf("NewData(MessageDelivery, server, offset=%d, key=%q, value=%q) error = %v, want nil", tt.offset, tt.key, tt.value, err)
				return
			}
			frame, err := protocol.Encode(data)
			wantFrame := `{"opcode":6,"emitter":1,"metadata":{},"payload":` + tt.json + `}`
			if err != nil || string(frame) != wantFrame {
				t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
				return
			}
			decoded, err := protocol.Decode(frame)
			if err != nil || !reflect.DeepEqual(decoded, data) {
				t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, decoded, err, data)
			}
			got, ok := decoded.Payload().(*protocol.MessageDeliveryPayload)
			if !ok {
				t.Errorf("Decode(%q).Payload() type = %T, want *MessageDeliveryPayload", frame, decoded.Payload())
				return
			}
			delivered := got.Message()
			if delivered.Offset() != tt.offset || !bytes.Equal(delivered.Key(), tt.key) || !bytes.Equal(delivered.Value(), tt.value) {
				t.Errorf("Decode(%q).Message() = (offset=%d, key=%q, value=%q), want (%d, %q, %q)", frame, delivered.Offset(), delivered.Key(), delivered.Value(), tt.offset, tt.key, tt.value)
			}
		})
	}
}

// TestMessageDeliveryZeroValue checks that the initial queue position and empty content are usable.
func TestMessageDeliveryZeroValue(t *testing.T) {
	payload := &protocol.MessageDeliveryPayload{}
	data, err := protocol.NewData(protocol.MessageDelivery, protocol.NewServerMetadata(), payload)
	if err != nil {
		t.Errorf("NewData(MessageDelivery, server, %#v) error = %v, want nil", payload, err)
		return
	}
	frame, err := protocol.Encode(data)
	wantFrame := `{"opcode":6,"emitter":1,"metadata":{},"payload":{"message":{"key":"","value":"","offset":0}}}`
	if err != nil || string(frame) != wantFrame {
		t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
	}
}

// TestMessageDeliveryPreservesInputOffset checks that assigning a delivery offset leaves the input intact.
func TestMessageDeliveryPreservesInputOffset(t *testing.T) {
	message := protocol.NewMessageDeliveryPayload(7, protocol.NewMessage(nil, nil)).Message()
	delivery := protocol.NewMessageDeliveryPayload(42, message)
	if got := message.Offset(); got != 7 {
		t.Errorf("NewMessageDeliveryPayload(42, message at 7) input offset = %d, want 7", got)
	}
	if got := delivery.Message().Offset(); got != 42 {
		t.Errorf("NewMessageDeliveryPayload(42, message at 7).Message().Offset() = %d, want 42", got)
	}
}

// TestNewDataRejectsInvalidMessageDelivery checks public validation and emitter inference.
func TestNewDataRejectsInvalidMessageDelivery(t *testing.T) {
	tests := []struct {
		name     string
		metadata protocol.Metadata
		payload  protocol.Payload
		wantErr  error
	}{
		{name: "nil payload", metadata: protocol.NewServerMetadata(), wantErr: protocol.ErrMissingPayload},
		{name: "nil payload pointer", metadata: protocol.NewServerMetadata(), payload: (*protocol.MessageDeliveryPayload)(nil), wantErr: protocol.ErrMissingPayload},
		{name: "oversized message", metadata: protocol.NewServerMetadata(), payload: protocol.NewMessageDeliveryPayload(0, protocol.NewMessage([]byte("k"), []byte(strings.Repeat("v", protocol.MessageSize)))), wantErr: protocol.ErrMessageTooLarge},
		{name: "client metadata", metadata: protocol.NewClientMetadata("client-1", "/client-1"), payload: protocol.NewMessageDeliveryPayload(0, protocol.NewMessage(nil, nil)), wantErr: protocol.ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protocol.NewData(protocol.MessageDelivery, tt.metadata, tt.payload)
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(got, protocol.Data{}) {
				t.Errorf("NewData(MessageDelivery, %#v, %#v) = %#v, error = %v, want zero Data and %v", tt.metadata, tt.payload, got, err, tt.wantErr)
			}
		})
	}
}

// TestProduceMessageOmitsDeliveryOffset checks that reusing a delivered message cannot send an offset.
func TestProduceMessageOmitsDeliveryOffset(t *testing.T) {
	message := protocol.NewMessageDeliveryPayload(42, protocol.NewMessage([]byte("k"), []byte("v"))).Message()
	payload := protocol.NewProduceMessagePayload("orders", message)
	data, err := protocol.NewData(protocol.ProduceMessage, protocol.NewClientMetadata("client-1", "/client-1"), payload)
	if err != nil {
		t.Errorf("NewData(ProduceMessage, client, message at 42) error = %v, want nil", err)
		return
	}
	frame, err := protocol.Encode(data)
	wantFrame := `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"key":"aw==","value":"dg=="}}}`
	if err != nil || string(frame) != wantFrame {
		t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
	}
}
