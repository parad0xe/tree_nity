package protocol

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestNewDataFromFrame checks every opcode for client and server frames.
func TestNewDataFromFrame(t *testing.T) {
	payloads := []struct {
		name    string
		opcode  op
		payload string
		want    Payload
	}{
		{name: "list topic", opcode: list_topic, payload: `{}`, want: &ListTopicPayload{}},
		{name: "create topic", opcode: create_topic, payload: `{}`, want: &CreateTopicPayload{}},
		{name: "consumer info", opcode: consumer_info, payload: `{}`, want: &ConsumerInfoPayload{}},
		{name: "topic subscription", opcode: topic_subsription, payload: `{}`, want: &TopicSubscriptionPayload{}},
		{name: "acknowledgement", opcode: aknowledgement, payload: `{}`, want: &AcknowledgementPayload{}},
		{name: "check topic", opcode: check_topic, payload: `{}`, want: &CheckTopicPayload{}},
		{
			name:    "produce raw message",
			opcode:  produce_message,
			payload: `{"raw":true,"message":{"key":"key","value":"value"}}`,
			want:    &ProduceMessagePayload{raw: true, message: Message{key: "key", value: "value"}},
		},
		{
			name:    "produce empty non-raw message",
			opcode:  produce_message,
			payload: `{"raw":false,"message":{"key":"","value":""}}`,
			want:    &ProduceMessagePayload{},
		},
		{
			name:    "produce message at byte limit",
			opcode:  produce_message,
			payload: fmt.Sprintf(`{"raw":false,"message":{"key":"","value":%q}}`, strings.Repeat("é", 512)),
			want:    &ProduceMessagePayload{message: Message{value: strings.Repeat("é", 512)}},
		},
		{name: "consumer disconnect", opcode: consumer_disconnect, payload: `{}`, want: &ConsumerDisconnectPayload{}},
		{name: "consumer heartbeat", opcode: consumer_heartbeat, payload: `{}`, want: &ConsumerHeartbeatPayload{}},
	}
	emitters := []struct {
		name     string
		emitter  emitter
		metadata string
		want     Metadata
	}{
		{
			name:     "client",
			emitter:  client,
			metadata: `{"clientId":"client-1","clientIPC":"/client-1"}`,
			want:     ClientMetadata{clientId: "client-1", clientIPC: "/client-1"},
		},
		{name: "server", emitter: server, metadata: `{}`, want: ServerMetadata{}},
	}

	for _, p := range payloads {
		for _, e := range emitters {
			t.Run(p.name+"/"+e.name, func(t *testing.T) {
				frame := fmt.Sprintf(" \n\t"+`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`+"\r\n\t", p.opcode, e.emitter, e.metadata, p.payload)
				want := Data{opcode: p.opcode, emitter: e.emitter, metadata: e.want, payload: p.want}

				got, err := NewDataFromFrame([]byte(frame))
				if err != nil {
					t.Errorf("NewDataFromFrame(%q) error = %v, want nil", frame, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("NewDataFromFrame(%q) = %#v, want %#v", frame, got, want)
				}
			})
		}
	}
}

// TestNewDataFromFrameRejectsInvalidInput checks malformed and incomplete frames.
func TestNewDataFromFrameRejectsInvalidInput(t *testing.T) {
	validFrame := `{"opcode":0,"emitter":1,"metadata":{},"payload":{}}`
	tests := []struct {
		name  string
		input string
	}{
		{name: "empty frame"},
		{name: "whitespace", input: " \n\t"},
		{name: "truncated JSON", input: `{"opcode":0`},
		{name: "invalid JSON", input: `{"opcode":}`},
		{name: "null frame", input: `null`},
		{name: "array frame", input: `[]`},
		{name: "string frame", input: `"frame"`},
		{name: "number frame", input: `1`},
		{name: "boolean frame", input: `false`},
		{name: "empty object", input: `{}`},
		{name: "unknown frame field", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":{},"extra":true}`},
		{name: "missing opcode", input: `{"emitter":1,"metadata":{},"payload":{}}`},
		{name: "null opcode", input: `{"opcode":null,"emitter":1,"metadata":{},"payload":{}}`},
		{name: "negative opcode", input: `{"opcode":-1,"emitter":1,"metadata":{},"payload":{}}`},
		{name: "unknown opcode", input: `{"opcode":9,"emitter":1,"metadata":{},"payload":{}}`},
		{name: "string opcode", input: `{"opcode":"0","emitter":1,"metadata":{},"payload":{}}`},
		{name: "fractional opcode", input: `{"opcode":0.5,"emitter":1,"metadata":{},"payload":{}}`},
		{name: "missing emitter", input: `{"opcode":0,"metadata":{},"payload":{}}`},
		{name: "null emitter", input: `{"opcode":0,"emitter":null,"metadata":{},"payload":{}}`},
		{name: "negative emitter", input: `{"opcode":0,"emitter":-1,"metadata":{},"payload":{}}`},
		{name: "unknown emitter", input: `{"opcode":0,"emitter":2,"metadata":{},"payload":{}}`},
		{name: "string emitter", input: `{"opcode":0,"emitter":"1","metadata":{},"payload":{}}`},
		{name: "fractional emitter", input: `{"opcode":0,"emitter":1.5,"metadata":{},"payload":{}}`},
		{name: "missing metadata", input: `{"opcode":0,"emitter":1,"payload":{}}`},
		{name: "null metadata", input: `{"opcode":0,"emitter":1,"metadata":null,"payload":{}}`},
		{name: "array metadata", input: `{"opcode":0,"emitter":1,"metadata":[],"payload":{}}`},
		{name: "unknown metadata field", input: `{"opcode":0,"emitter":1,"metadata":{"extra":true},"payload":{}}`},
		{name: "missing clientId", input: `{"opcode":0,"emitter":0,"metadata":{"clientIPC":"/client"},"payload":{}}`},
		{name: "missing clientIPC", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":"client"},"payload":{}}`},
		{name: "missing payload", input: `{"opcode":0,"emitter":1,"metadata":{}}`},
		{name: "null payload", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":null}`},
		{name: "array payload", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":[]}`},
		{name: "unknown payload field", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":{"extra":true}}`},
		{name: "missing raw", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"message":{"key":"k","value":"v"}}}`},
		{name: "missing message", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":false}}`},
		{name: "missing message key", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":true,"message":{"value":"v"}}}`},
		{name: "missing message value", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":true,"message":{"key":"k"}}}`},
		{name: "unknown message field", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":true,"message":{"key":"k","value":"v","extra":true}}}`},
		{
			name:  "oversized message",
			input: fmt.Sprintf(`{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":true,"message":{"key":"k","value":%q}}}`, strings.Repeat("v", 1024)),
		},
		{name: "additional object", input: validFrame + `{}`},
		{name: "additional null", input: validFrame + ` null`},
		{name: "additional scalar", input: validFrame + ` true`},
		{name: "truncated trailing JSON", input: validFrame + ` {`},
		{name: "trailing garbage", input: validFrame + ` garbage`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDataFromFrame([]byte(tt.input))
			if err == nil {
				t.Errorf("NewDataFromFrame(%q) error = nil, want an error", tt.input)
			}
			if !reflect.DeepEqual(got, Data{}) {
				t.Errorf("NewDataFromFrame(%q) = %#v, want %#v", tt.input, got, Data{})
			}
		})
	}
}
