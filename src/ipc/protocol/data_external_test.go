package protocol_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"tree_nity/src/ipc/protocol"
)

// TestNewData checks public construction, emitter inference, and codec round trips.
func TestNewData(t *testing.T) {
	payloads := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
		json    string
	}{
		{name: "list topic", opcode: protocol.ListTopic, payload: protocol.NewListTopicPayload(), json: `{}`},
		{name: "create topic", opcode: protocol.CreateTopic, payload: protocol.NewCreateTopicPayload(), json: `{}`},
		{name: "consumer info", opcode: protocol.ConsumerInfo, payload: protocol.NewConsumerInfoPayload(), json: `{}`},
		{name: "topic subscription", opcode: protocol.TopicSubscription, payload: protocol.NewTopicSubscriptionPayload(), json: `{}`},
		{name: "acknowledgement", opcode: protocol.Acknowledgement, payload: protocol.NewAcknowledgementPayload(), json: `{}`},
		{name: "check topic", opcode: protocol.CheckTopic, payload: protocol.NewCheckTopicPayload(), json: `{}`},
		{
			name: "produce message", opcode: protocol.ProduceMessage,
			payload: protocol.NewProduceMessagePayload(true, protocol.NewMessage("key", "value")),
			json:    `{"raw":true,"message":{"key":"key","value":"value"}}`,
		},
		{name: "consumer disconnect", opcode: protocol.ConsumerDisconnect, payload: protocol.NewConsumerDisconnectPayload(), json: `{}`},
		{name: "consumer heartbeat", opcode: protocol.ConsumerHeartbeat, payload: protocol.NewConsumerHeartbeatPayload(), json: `{}`},
	}
	emitters := []struct {
		name     string
		metadata protocol.Metadata
		emitter  protocol.Emitter
	}{
		{name: "client", metadata: protocol.NewClientMetadata("client-1", "/client-1"), emitter: protocol.Client},
	}
	for _, p := range payloads {
		for _, e := range emitters {
			t.Run(p.name+"/"+e.name, func(t *testing.T) {
				data, err := protocol.NewData(p.opcode, e.metadata, p.payload)
				if err != nil {
					t.Errorf("NewData(%d, %#v, %#v) error = %v, want nil", p.opcode, e.metadata, p.payload, err)
					return
				}
				if data.Opcode() != p.opcode || data.Emitter() != e.emitter {
					t.Errorf("NewData(%d, %#v, %#v) codes = (%d, %d), want (%d, %d)", p.opcode, e.metadata, p.payload, data.Opcode(), data.Emitter(), p.opcode, e.emitter)
				}
				if !reflect.DeepEqual(data.Metadata(), e.metadata) || !reflect.DeepEqual(data.Payload(), p.payload) {
					t.Errorf("NewData(%d, %#v, %#v) contents = (%#v, %#v), want (%#v, %#v)", p.opcode, e.metadata, p.payload, data.Metadata(), data.Payload(), e.metadata, p.payload)
				}
				frame, err := protocol.Encode(data)
				if err != nil {
					t.Errorf("Encode(%#v) error = %v, want nil", data, err)
					return
				}
				var envelope struct {
					Payload json.RawMessage `json:"payload"`
				}
				if err := json.Unmarshal(frame, &envelope); err != nil {
					t.Errorf("Unmarshal(%q) error = %v, want nil", frame, err)
					return
				}
				if string(envelope.Payload) != p.json {
					t.Errorf("Encode(%#v) payload = %s, want %s", data, envelope.Payload, p.json)
				}
				decoded, err := protocol.Decode(frame)
				if err != nil || !reflect.DeepEqual(decoded, data) {
					t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, decoded, err, data)
				}
				if metadata, ok := decoded.Metadata().(*protocol.ClientMetadata); ok {
					if metadata.ClientID() != "client-1" || metadata.ClientIPC() != "/client-1" {
						t.Errorf("Decode(%q) client metadata = (%q, %q), want (%q, %q)", frame, metadata.ClientID(), metadata.ClientIPC(), "client-1", "/client-1")
					}
				}
			})
		}
	}
}

// TestNewDataRejectsInvalidInput checks that invalid frames cannot be constructed.
func TestNewDataRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		opcode   protocol.Opcode
		metadata protocol.Metadata
		payload  protocol.Payload
		wantErr  error
	}{
		{name: "nil metadata", payload: protocol.NewListTopicPayload(), wantErr: protocol.ErrMissingMetadata},
		{name: "nil client metadata", metadata: (*protocol.ClientMetadata)(nil), payload: protocol.NewListTopicPayload(), wantErr: protocol.ErrMissingMetadata},
		{name: "nil server metadata", metadata: (*protocol.ServerMetadata)(nil), payload: protocol.NewListTopicPayload(), wantErr: protocol.ErrMissingMetadata},
		{name: "nil payload", metadata: protocol.NewClientMetadata("client-1", "/client-1"), wantErr: protocol.ErrMissingPayload},
		{name: "nil payload pointer", opcode: protocol.CreateTopic, metadata: protocol.NewClientMetadata("client-1", "/client-1"), payload: (*protocol.CreateTopicPayload)(nil), wantErr: protocol.ErrMissingPayload},
		{name: "negative opcode", opcode: -1, metadata: protocol.NewClientMetadata("client-1", "/client-1"), payload: protocol.NewListTopicPayload(), wantErr: protocol.ErrUnknownOpcode},
		{name: "unknown opcode", opcode: 9, metadata: protocol.NewClientMetadata("client-1", "/client-1"), payload: protocol.NewListTopicPayload(), wantErr: protocol.ErrUnknownOpcode},
		{name: "incompatible payload", metadata: protocol.NewClientMetadata("client-1", "/client-1"), payload: protocol.NewCreateTopicPayload(), wantErr: protocol.ErrInvalidPayload},
		{name: "invalid client ID", metadata: protocol.NewClientMetadata("\xff", "/client"), payload: protocol.NewListTopicPayload(), wantErr: protocol.ErrInvalidMetadata},
		{name: "invalid client IPC", metadata: protocol.NewClientMetadata("client", "\xff"), payload: protocol.NewListTopicPayload(), wantErr: protocol.ErrInvalidMetadata},
		{
			name: "invalid message key", opcode: protocol.ProduceMessage, metadata: protocol.NewClientMetadata("client-1", "/client-1"),
			payload: protocol.NewProduceMessagePayload(false, protocol.NewMessage("\xff", "value")), wantErr: protocol.ErrInvalidPayload,
		},
		{
			name: "invalid message value", opcode: protocol.ProduceMessage, metadata: protocol.NewClientMetadata("client-1", "/client-1"),
			payload: protocol.NewProduceMessagePayload(false, protocol.NewMessage("key", "\xff")), wantErr: protocol.ErrInvalidPayload,
		},
		{
			name: "oversized message", opcode: protocol.ProduceMessage, metadata: protocol.NewClientMetadata("client-1", "/client-1"),
			payload: protocol.NewProduceMessagePayload(false, protocol.NewMessage("k", strings.Repeat("v", protocol.MessageSize))), wantErr: protocol.ErrMessageTooLarge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := protocol.NewData(tt.opcode, tt.metadata, tt.payload)
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(data, protocol.Data{}) {
				t.Errorf("NewData(%d, %#v, %#v) = %#v, error = %v, want zero Data and %v", tt.opcode, tt.metadata, tt.payload, data, err, tt.wantErr)
			}
		})
	}
}

// TestNewDataRejectsServerOpcodes checks emitter inference against the empty server opcode list.
func TestNewDataRejectsServerOpcodes(t *testing.T) {
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
	}{
		{name: "list topic", opcode: protocol.ListTopic, payload: protocol.NewListTopicPayload()},
		{name: "create topic", opcode: protocol.CreateTopic, payload: protocol.NewCreateTopicPayload()},
		{name: "consumer info", opcode: protocol.ConsumerInfo, payload: protocol.NewConsumerInfoPayload()},
		{name: "topic subscription", opcode: protocol.TopicSubscription, payload: protocol.NewTopicSubscriptionPayload()},
		{name: "acknowledgement", opcode: protocol.Acknowledgement, payload: protocol.NewAcknowledgementPayload()},
		{name: "check topic", opcode: protocol.CheckTopic, payload: protocol.NewCheckTopicPayload()},
		{
			name: "produce message", opcode: protocol.ProduceMessage,
			payload: protocol.NewProduceMessagePayload(true, protocol.NewMessage("key", "value")),
		},
		{name: "consumer disconnect", opcode: protocol.ConsumerDisconnect, payload: protocol.NewConsumerDisconnectPayload()},
		{name: "consumer heartbeat", opcode: protocol.ConsumerHeartbeat, payload: protocol.NewConsumerHeartbeatPayload()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := protocol.NewServerMetadata()
			got, err := protocol.NewData(tt.opcode, metadata, tt.payload)
			if !errors.Is(err, protocol.ErrUnknownOpcode) || !reflect.DeepEqual(got, protocol.Data{}) {
				t.Errorf("NewData(%d, %#v, %#v) = %#v, error = %v, want zero Data and %v", tt.opcode, metadata, tt.payload, got, err, protocol.ErrUnknownOpcode)
			}
		})
	}
}

// TestProduceMessagePayloadAccessors checks payload and message accessors after a round trip.
func TestProduceMessagePayloadAccessors(t *testing.T) {
	tests := []struct {
		name  string
		raw   bool
		key   string
		value string
	}{
		{name: "empty"},
		{name: "minimal", key: "k", value: "v"},
		{name: "raw", raw: true, key: "key", value: "value"},
		{name: "byte limit", value: strings.Repeat("v", protocol.MessageSize)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := protocol.NewMessage(tt.key, tt.value)
			payload := protocol.NewProduceMessagePayload(tt.raw, message)
			data, err := protocol.NewData(protocol.ProduceMessage, protocol.NewClientMetadata("client-1", "/client-1"), payload)
			if err != nil {
				t.Errorf("NewData(ProduceMessage, client, %#v) error = %v, want nil", payload, err)
				return
			}
			frame, err := protocol.Encode(data)
			if err != nil {
				t.Errorf("Encode(%#v) error = %v, want nil", data, err)
				return
			}
			decoded, err := protocol.Decode(frame)
			if err != nil {
				t.Errorf("Decode(%q) error = %v, want nil", frame, err)
				return
			}
			got, ok := decoded.Payload().(*protocol.ProduceMessagePayload)
			if !ok {
				t.Errorf("Decode(%q).Payload() type = %T, want *ProduceMessagePayload", frame, decoded.Payload())
				return
			}
			gotMessage := got.Message()
			if got.Raw() != tt.raw || gotMessage.Key() != tt.key || gotMessage.Value() != tt.value || gotMessage.Offset() != 0 {
				t.Errorf("Decode(%q) message payload = (%t, %q, %q, %d), want (%t, %q, %q, 0)", frame, got.Raw(), gotMessage.Key(), gotMessage.Value(), gotMessage.Offset(), tt.raw, tt.key, tt.value)
			}
		})
	}
}
