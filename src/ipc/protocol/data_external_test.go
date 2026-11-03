package protocol_test

import (
	"encoding/json"
	"errors"
	"fmt"
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
		{name: "create topic", opcode: protocol.CreateTopic, payload: protocol.NewCreateTopicPayload("orders"), json: `{"topic":"orders"}`},
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
		{name: "incompatible payload", metadata: protocol.NewClientMetadata("client-1", "/client-1"), payload: protocol.NewCreateTopicPayload("orders"), wantErr: protocol.ErrInvalidPayload},
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

// TestNewDataRejectsClientPayloadsFromServer checks emitter inference against client payloads.
func TestNewDataRejectsClientPayloadsFromServer(t *testing.T) {
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
		wantErr error
	}{
		{name: "list topic", opcode: protocol.ListTopic, payload: protocol.NewListTopicPayload(), wantErr: protocol.ErrInvalidPayload},
		{name: "create topic", opcode: protocol.CreateTopic, payload: protocol.NewCreateTopicPayload("orders"), wantErr: protocol.ErrInvalidPayload},
		{name: "consumer info", opcode: protocol.ConsumerInfo, payload: protocol.NewConsumerInfoPayload(), wantErr: protocol.ErrInvalidPayload},
		{name: "topic subscription", opcode: protocol.TopicSubscription, payload: protocol.NewTopicSubscriptionPayload(), wantErr: protocol.ErrInvalidPayload},
		{name: "acknowledgement", opcode: protocol.Acknowledgement, payload: protocol.NewAcknowledgementPayload(), wantErr: protocol.ErrInvalidPayload},
		{name: "check topic", opcode: protocol.CheckTopic, payload: protocol.NewCheckTopicPayload(), wantErr: protocol.ErrInvalidPayload},
		{
			name: "produce message", opcode: protocol.ProduceMessage,
			payload: protocol.NewProduceMessagePayload(true, protocol.NewMessage("key", "value")),
			wantErr: protocol.ErrInvalidPayload,
		},
		{name: "consumer disconnect", opcode: protocol.ConsumerDisconnect, payload: protocol.NewConsumerDisconnectPayload(), wantErr: protocol.ErrInvalidPayload},
		{name: "consumer heartbeat", opcode: protocol.ConsumerHeartbeat, payload: protocol.NewConsumerHeartbeatPayload(), wantErr: protocol.ErrUnknownOpcode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := protocol.NewServerMetadata()
			got, err := protocol.NewData(tt.opcode, metadata, tt.payload)
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(got, protocol.Data{}) {
				t.Errorf("NewData(%d, %#v, %#v) = %#v, error = %v, want zero Data and %v", tt.opcode, metadata, tt.payload, got, err, tt.wantErr)
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

// TestErrorReplyRoundTrip checks public construction, wire encoding, and decoded accessors.
func TestErrorReplyRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		kind   protocol.ErrorKind
		reason string
		json   string
	}{
		{name: "empty reason", kind: protocol.InvalidRequest, json: `{"kind":0,"reason":""}`},
		{name: "invalid request", kind: protocol.InvalidRequest, reason: "invalid request", json: `{"kind":0,"reason":"invalid request"}`},
		{name: "topic not found", kind: protocol.TopicNotFound, reason: "topic not found", json: `{"kind":1,"reason":"topic not found"}`},
		{name: "topic exists", kind: protocol.TopicExists, reason: "topic exists", json: `{"kind":2,"reason":"topic exists"}`},
		{name: "client not found", kind: protocol.ClientNotFound, reason: "client not found", json: `{"kind":3,"reason":"client not found"}`},
		{name: "client name taken", kind: protocol.ClientNameTaken, reason: "client name taken", json: `{"kind":4,"reason":"client name taken"}`},
		{name: "escaped UTF-8 reason", kind: protocol.InvalidRequest, reason: "échec\n\"topic\"", json: `{"kind":0,"reason":"échec\n\"topic\""}`},
		{name: "large reason", kind: protocol.InvalidRequest, reason: strings.Repeat("r", 4096), json: `{"kind":0,"reason":"` + strings.Repeat("r", 4096) + `"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := protocol.NewErrorReplyPayload(tt.kind, tt.reason)
			data, err := protocol.NewData(protocol.ErrorReply, protocol.NewServerMetadata(), payload)
			if err != nil {
				t.Errorf("NewData(ErrorReply, server, %#v) error = %v, want nil", payload, err)
				return
			}
			frame, err := protocol.Encode(data)
			wantFrame := `{"opcode":5,"emitter":1,"metadata":{},"payload":` + tt.json + `}`
			if err != nil || string(frame) != wantFrame {
				t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
				return
			}
			decoded, err := protocol.Decode(frame)
			if err != nil || !reflect.DeepEqual(decoded, data) {
				t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, decoded, err, data)
			}
			got, ok := decoded.Payload().(*protocol.ErrorReplyPayload)
			if !ok {
				t.Errorf("Decode(%q).Payload() type = %T, want *ErrorReplyPayload", frame, decoded.Payload())
				return
			}
			if got.Kind() != tt.kind || got.Reason() != tt.reason {
				t.Errorf("Decode(%q) error reply = (%d, %q), want (%d, %q)", frame, got.Kind(), got.Reason(), tt.kind, tt.reason)
			}
		})
	}
}

// TestNewDataRejectsInvalidErrorReply checks validation through the public constructor.
func TestNewDataRejectsInvalidErrorReply(t *testing.T) {
	tests := []struct {
		name     string
		metadata protocol.Metadata
		payload  protocol.Payload
		wantErr  error
	}{
		{name: "nil payload", metadata: protocol.NewServerMetadata(), wantErr: protocol.ErrMissingPayload},
		{name: "nil payload pointer", metadata: protocol.NewServerMetadata(), payload: (*protocol.ErrorReplyPayload)(nil), wantErr: protocol.ErrMissingPayload},
		{name: "negative kind", metadata: protocol.NewServerMetadata(), payload: protocol.NewErrorReplyPayload(-1, "error"), wantErr: protocol.ErrInvalidPayload},
		{name: "unknown kind", metadata: protocol.NewServerMetadata(), payload: protocol.NewErrorReplyPayload(5, "error"), wantErr: protocol.ErrInvalidPayload},
		{name: "invalid UTF-8 reason", metadata: protocol.NewServerMetadata(), payload: protocol.NewErrorReplyPayload(protocol.InvalidRequest, "\xff"), wantErr: protocol.ErrInvalidPayload},
		{name: "client metadata", metadata: protocol.NewClientMetadata("client-1", "/client-1"), payload: protocol.NewErrorReplyPayload(protocol.InvalidRequest, "error"), wantErr: protocol.ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protocol.NewData(protocol.ErrorReply, tt.metadata, tt.payload)
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(got, protocol.Data{}) {
				t.Errorf("NewData(ErrorReply, %#v, %#v) = %#v, error = %v, want zero Data and %v", tt.metadata, tt.payload, got, err, tt.wantErr)
			}
		})
	}
}

// TestCreateTopicPayloadRoundTrip checks topic names and the public accessor after decoding.
func TestCreateTopicPayloadRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{name: "minimal name", topic: "t"},
		{name: "topic name", topic: "orders"},
		{name: "maximum length", topic: strings.Repeat("t", 32)},
		{name: "mixed characters", topic: "User_09.events-1"},
		{name: "punctuation", topic: "_.-"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := protocol.NewCreateTopicPayload(tt.topic)
			metadata := protocol.NewClientMetadata("client-1", "/client-1")
			data, err := protocol.NewData(protocol.CreateTopic, metadata, payload)
			if err != nil {
				t.Errorf("NewData(CreateTopic, %#v, topic=%q) error = %v, want nil", metadata, tt.topic, err)
				return
			}
			frame, err := protocol.Encode(data)
			wantFrame := fmt.Sprintf(`{"opcode":1,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":%q}}`, tt.topic)
			if err != nil || string(frame) != wantFrame {
				t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
				return
			}
			decoded, err := protocol.Decode(frame)
			if err != nil || !reflect.DeepEqual(decoded, data) {
				t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, decoded, err, data)
			}
			got, ok := decoded.Payload().(*protocol.CreateTopicPayload)
			if !ok {
				t.Errorf("Decode(%q).Payload() type = %T, want *CreateTopicPayload", frame, decoded.Payload())
				return
			}
			if got.Topic() != tt.topic {
				t.Errorf("Decode(%q).Payload().Topic() = %q, want %q", frame, got.Topic(), tt.topic)
			}
		})
	}
}

// TestCreateTopicRejectsInvalidTopicNames checks name validation in both directions.
func TestCreateTopicRejectsInvalidTopicNames(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{name: "empty name"},
		{name: "33 characters", topic: strings.Repeat("t", 33)},
		{name: "large name", topic: strings.Repeat("t", 4096)},
		{name: "space", topic: "user events"},
		{name: "slash", topic: "user/events"},
		{name: "non-ASCII", topic: "événements"},
		{name: "newline", topic: "orders\n"},
		{name: "null byte", topic: "orders\x00"},
		{name: "invalid UTF-8", topic: "\xff"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := protocol.NewCreateTopicPayload(tt.topic)
			metadata := protocol.NewClientMetadata("client-1", "/client-1")
			data, err := protocol.NewData(protocol.CreateTopic, metadata, payload)
			if !errors.Is(err, protocol.ErrInvalidPayload) || !reflect.DeepEqual(data, protocol.Data{}) {
				t.Errorf("NewData(CreateTopic, %#v, topic=%q) = %#v, error = %v, want zero Data and %v", metadata, tt.topic, data, err, protocol.ErrInvalidPayload)
			}
			topicJSON, err := json.Marshal(tt.topic)
			if err != nil {
				t.Errorf("Marshal(%q) error = %v, want nil", tt.topic, err)
				return
			}
			frame := `{"opcode":1,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":` + string(topicJSON) + `}}`
			decoded, err := protocol.Decode([]byte(frame))
			if !errors.Is(err, protocol.ErrInvalidPayload) || !reflect.DeepEqual(decoded, protocol.Data{}) {
				t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and %v", frame, decoded, err, protocol.ErrInvalidPayload)
			}
		})
	}
}

// TestCreateTopicReplyRoundTrip checks constructed and zero-value success responses.
func TestCreateTopicReplyRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		payload *protocol.CreateTopicReplyPayload
	}{
		{name: "constructor", payload: protocol.NewCreateTopicReplyPayload()},
		{name: "zero value", payload: &protocol.CreateTopicReplyPayload{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := protocol.NewServerMetadata()
			data, err := protocol.NewData(protocol.CreateTopicReply, metadata, tt.payload)
			if err != nil {
				t.Errorf("NewData(CreateTopicReply, %#v, %#v) error = %v, want nil", metadata, tt.payload, err)
				return
			}
			frame, err := protocol.Encode(data)
			wantFrame := `{"opcode":1,"emitter":1,"metadata":{},"payload":{}}`
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

// TestNewDataRejectsCreateTopicReplyFromClient checks the opcode shared with CreateTopic.
func TestNewDataRejectsCreateTopicReplyFromClient(t *testing.T) {
	metadata := protocol.NewClientMetadata("client-1", "/client-1")
	payload := protocol.NewCreateTopicReplyPayload()
	got, err := protocol.NewData(protocol.CreateTopicReply, metadata, payload)
	if !errors.Is(err, protocol.ErrInvalidPayload) || !reflect.DeepEqual(got, protocol.Data{}) {
		t.Errorf("NewData(CreateTopicReply, %#v, %#v) = %#v, error = %v, want zero Data and %v", metadata, payload, got, err, protocol.ErrInvalidPayload)
	}
}
