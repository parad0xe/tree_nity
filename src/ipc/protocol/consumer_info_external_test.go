package protocol_test

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"tree_nity/src/ipc/protocol"
)

// TestConsumerInfoRoundTrip checks that the queried subscriber is independent of the caller.
func TestConsumerInfoRoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		subscriber string
	}{
		{name: "minimal name", subscriber: "c"},
		{name: "different subscriber", subscriber: "consumer-1"},
		{name: "maximum length", subscriber: strings.Repeat("c", 32)},
		{name: "mixed characters", subscriber: "Consumer_09.events-1"},
		{name: "punctuation", subscriber: "_.-"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := protocol.NewConsumerInfoPayload("caller-1", tt.subscriber)
			data, err := protocol.NewData(protocol.ConsumerInfo, protocol.NewClientMetadata("/caller-1"), payload)
			if err != nil {
				t.Errorf("NewData(ConsumerInfo, caller-1, subscriber=%q) error = %v, want nil", tt.subscriber, err)
				return
			}
			frame, err := protocol.Encode(data)
			wantFrame := fmt.Sprintf(`{"opcode":2,"emitter":0,"metadata":{"clientIPC":"/caller-1"},"payload":{"clientId":"caller-1","subscriber":%q}}`, tt.subscriber)
			if err != nil || string(frame) != wantFrame {
				t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
				return
			}
			decoded, err := protocol.Decode(frame)
			if err != nil || !reflect.DeepEqual(decoded, data) {
				t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, decoded, err, data)
			}
			got, ok := decoded.Payload().(*protocol.ConsumerInfoPayload)
			if !ok {
				t.Errorf("Decode(%q).Payload() type = %T, want *ConsumerInfoPayload", frame, decoded.Payload())
				return
			}
			if got.Subscriber() != tt.subscriber {
				t.Errorf("Decode(%q).Subscriber() = %q, want %q", frame, got.Subscriber(), tt.subscriber)
			}
		})
	}
}

// TestConsumerInfoReplyRoundTrip checks field order, empty strings, offsets, and public accessors.
func TestConsumerInfoReplyRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		client string
		topic  string
		offset uint32
		prefix string
		ipc    string
		json   string
	}{
		{name: "minimal with zero and empty fields", client: "c", topic: "t", json: `{"client":"c","topic":"t","offset":0,"prefix":"","ipc":""}`},
		{name: "maximum signed offset", client: "c", topic: "t", offset: 2147483647, json: `{"client":"c","topic":"t","offset":2147483647,"prefix":"","ipc":""}`},
		{name: "above signed limit", client: "c", topic: "t", offset: 2147483648, json: `{"client":"c","topic":"t","offset":2147483648,"prefix":"","ipc":""}`},
		{name: "subscription", client: "consumer-1", topic: "orders", offset: 43, prefix: "user:", ipc: "/consumer-1", json: `{"client":"consumer-1","topic":"orders","offset":43,"prefix":"user:","ipc":"/consumer-1"}`},
		{name: "maximum names and offset", client: strings.Repeat("c", 32), topic: strings.Repeat("t", 32), offset: 4294967295, json: `{"client":"` + strings.Repeat("c", 32) + `","topic":"` + strings.Repeat("t", 32) + `","offset":4294967295,"prefix":"","ipc":""}`},
		{name: "UTF-8 strings", client: "Consumer_09.events-1", topic: "User_09.events-1", prefix: "événement/", ipc: "/tmp/猫", json: `{"client":"Consumer_09.events-1","topic":"User_09.events-1","offset":0,"prefix":"événement/","ipc":"/tmp/猫"}`},
		{name: "escaped strings", client: "c", topic: "t", prefix: "\"\\\n\x00", ipc: "/queue\"\\\n", json: `{"client":"c","topic":"t","offset":0,"prefix":"\"\\\n\u0000","ipc":"/queue\"\\\n"}`},
		{name: "large text fields", client: "c", topic: "t", prefix: strings.Repeat("p", 4096), ipc: strings.Repeat("q", 4096), json: `{"client":"c","topic":"t","offset":0,"prefix":"` + strings.Repeat("p", 4096) + `","ipc":"` + strings.Repeat("q", 4096) + `"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := protocol.NewConsumerInfoReplyPayload(tt.client, tt.topic, tt.offset, tt.prefix, tt.ipc)
			data, err := protocol.NewData(protocol.ConsumerInfoReply, protocol.NewServerMetadata(), payload)
			if err != nil {
				t.Errorf("NewData(ConsumerInfoReply, server, %#v) error = %v, want nil", payload, err)
				return
			}
			frame, err := protocol.Encode(data)
			wantFrame := `{"opcode":2,"emitter":1,"metadata":{},"payload":` + tt.json + `}`
			if err != nil || string(frame) != wantFrame {
				t.Errorf("Encode(%#v) = %q, error = %v, want %q and nil", data, frame, err, wantFrame)
				return
			}
			decoded, err := protocol.Decode(frame)
			if err != nil || !reflect.DeepEqual(decoded, data) {
				t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, decoded, err, data)
			}
			got, ok := decoded.Payload().(*protocol.ConsumerInfoReplyPayload)
			if !ok {
				t.Errorf("Decode(%q).Payload() type = %T, want *ConsumerInfoReplyPayload", frame, decoded.Payload())
				return
			}
			if got.Client() != tt.client || got.Topic() != tt.topic || got.Offset() != tt.offset || got.Prefix() != tt.prefix || got.IPC() != tt.ipc {
				t.Errorf("Decode(%q) consumer info = (%q, %q, %d, %q, %q), want (%q, %q, %d, %q, %q)", frame, got.Client(), got.Topic(), got.Offset(), got.Prefix(), got.IPC(), tt.client, tt.topic, tt.offset, tt.prefix, tt.ipc)
			}
		})
	}
}

// TestNewDataRejectsInvalidConsumerInfoPayloads checks validation at the public construction boundary.
func TestNewDataRejectsInvalidConsumerInfoPayloads(t *testing.T) {
	client := protocol.NewClientMetadata("/caller-1")
	server := protocol.NewServerMetadata()
	tests := []struct {
		name     string
		opcode   protocol.Opcode
		metadata protocol.Metadata
		payload  protocol.Payload
		wantErr  error
	}{
		{name: "nil request", opcode: protocol.ConsumerInfo, metadata: client, payload: (*protocol.ConsumerInfoPayload)(nil), wantErr: protocol.ErrMissingPayload},
		{name: "zero request", opcode: protocol.ConsumerInfo, metadata: client, payload: &protocol.ConsumerInfoPayload{}, wantErr: protocol.ErrInvalidPayload},
		{name: "invalid subscriber", opcode: protocol.ConsumerInfo, metadata: client, payload: protocol.NewConsumerInfoPayload("caller-1", "consumer/name"), wantErr: protocol.ErrInvalidPayload},
		{name: "nil reply", opcode: protocol.ConsumerInfoReply, metadata: server, payload: (*protocol.ConsumerInfoReplyPayload)(nil), wantErr: protocol.ErrMissingPayload},
		{name: "zero reply", opcode: protocol.ConsumerInfoReply, metadata: server, payload: &protocol.ConsumerInfoReplyPayload{}, wantErr: protocol.ErrInvalidPayload},
		{name: "empty reply topic", opcode: protocol.ConsumerInfoReply, metadata: server, payload: protocol.NewConsumerInfoReplyPayload("c", "", 0, "", ""), wantErr: protocol.ErrInvalidPayload},
		{name: "invalid prefix UTF-8", opcode: protocol.ConsumerInfoReply, metadata: server, payload: protocol.NewConsumerInfoReplyPayload("c", "t", 0, "\xff", ""), wantErr: protocol.ErrInvalidPayload},
		{name: "invalid IPC UTF-8", opcode: protocol.ConsumerInfoReply, metadata: server, payload: protocol.NewConsumerInfoReplyPayload("c", "t", 0, "", "\xff"), wantErr: protocol.ErrInvalidPayload},
		{name: "reply from client", opcode: protocol.ConsumerInfoReply, metadata: client, payload: protocol.NewConsumerInfoReplyPayload("c", "t", 0, "", ""), wantErr: protocol.ErrInvalidPayload},
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
