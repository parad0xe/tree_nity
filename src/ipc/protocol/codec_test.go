package protocol

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestDecode checks every opcode for client and server frames.
func TestDecode(t *testing.T) {
	payloads := []struct {
		name    string
		opcode  op
		payload string
		want    Payload
	}{
		{name: "list topic", opcode: listTopic, payload: `{}`, want: &ListTopicPayload{}},
		{name: "create topic", opcode: createTopic, payload: `{}`, want: &CreateTopicPayload{}},
		{name: "consumer info", opcode: consumerInfo, payload: `{}`, want: &ConsumerInfoPayload{}},
		{name: "topic subscription", opcode: topicSubscription, payload: `{}`, want: &TopicSubscriptionPayload{}},
		{name: "acknowledgement", opcode: acknowledgement, payload: `{}`, want: &AcknowledgementPayload{}},
		{name: "check topic", opcode: checkTopic, payload: `{}`, want: &CheckTopicPayload{}},
		{
			name:    "produce raw message",
			opcode:  produceMessage,
			payload: `{"raw":true,"message":{"key":"key","value":"value"}}`,
			want:    &ProduceMessagePayload{raw: true, message: Message{key: "key", value: "value"}},
		},
		{
			name:    "produce empty non-raw message",
			opcode:  produceMessage,
			payload: `{"raw":false,"message":{"key":"","value":""}}`,
			want:    &ProduceMessagePayload{},
		},
		{
			name:    "produce message at byte limit",
			opcode:  produceMessage,
			payload: fmt.Sprintf(`{"raw":false,"message":{"key":"","value":%q}}`, strings.Repeat("é", 512)),
			want:    &ProduceMessagePayload{message: Message{value: strings.Repeat("é", 512)}},
		},
		{name: "consumer disconnect", opcode: consumerDisconnect, payload: `{}`, want: &ConsumerDisconnectPayload{}},
		{name: "consumer heartbeat", opcode: consumerHeartbeat, payload: `{}`, want: &ConsumerHeartbeatPayload{}},
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

				got, err := Decode([]byte(frame))
				if err != nil {
					t.Errorf("Decode(%q) error = %v, want nil", frame, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("Decode(%q) = %#v, want %#v", frame, got, want)
				}
			})
		}
	}
}

// TestDecodeRejectsInvalidInput checks malformed and incomplete frames.
func TestDecodeRejectsInvalidInput(t *testing.T) {
	validFrame := `{"opcode":0,"emitter":1,"metadata":{},"payload":{}}`
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "empty frame"},
		{name: "whitespace", input: " \n\t"},
		{name: "truncated JSON", input: `{"opcode":0`},
		{name: "invalid JSON", input: `{"opcode":}`},
		{name: "null frame", input: `null`, wantErr: ErrMissingOpcode},
		{name: "array frame", input: `[]`},
		{name: "string frame", input: `"frame"`},
		{name: "number frame", input: `1`},
		{name: "boolean frame", input: `false`},
		{name: "empty object", input: `{}`, wantErr: ErrMissingOpcode},
		{name: "unknown frame field", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":{},"extra":true}`},
		{name: "missing opcode", input: `{"emitter":1,"metadata":{},"payload":{}}`, wantErr: ErrMissingOpcode},
		{name: "null opcode", input: `{"opcode":null,"emitter":1,"metadata":{},"payload":{}}`, wantErr: ErrMissingOpcode},
		{name: "negative opcode", input: `{"opcode":-1,"emitter":1,"metadata":{},"payload":{}}`, wantErr: ErrUnknownOpcode},
		{name: "unknown opcode", input: `{"opcode":9,"emitter":1,"metadata":{},"payload":{}}`, wantErr: ErrUnknownOpcode},
		{name: "string opcode", input: `{"opcode":"0","emitter":1,"metadata":{},"payload":{}}`},
		{name: "fractional opcode", input: `{"opcode":0.5,"emitter":1,"metadata":{},"payload":{}}`},
		{name: "missing emitter", input: `{"opcode":0,"metadata":{},"payload":{}}`, wantErr: ErrMissingEmitter},
		{name: "null emitter", input: `{"opcode":0,"emitter":null,"metadata":{},"payload":{}}`, wantErr: ErrMissingEmitter},
		{name: "negative emitter", input: `{"opcode":0,"emitter":-1,"metadata":{},"payload":{}}`, wantErr: ErrUnknownEmitter},
		{name: "unknown emitter", input: `{"opcode":0,"emitter":2,"metadata":{},"payload":{}}`, wantErr: ErrUnknownEmitter},
		{name: "string emitter", input: `{"opcode":0,"emitter":"1","metadata":{},"payload":{}}`},
		{name: "fractional emitter", input: `{"opcode":0,"emitter":1.5,"metadata":{},"payload":{}}`},
		{name: "missing metadata", input: `{"opcode":0,"emitter":1,"payload":{}}`, wantErr: ErrMissingMetadata},
		{name: "null metadata", input: `{"opcode":0,"emitter":1,"metadata":null,"payload":{}}`, wantErr: ErrMissingMetadata},
		{name: "array metadata", input: `{"opcode":0,"emitter":1,"metadata":[],"payload":{}}`},
		{name: "unknown metadata field", input: `{"opcode":0,"emitter":1,"metadata":{"extra":true},"payload":{}}`},
		{name: "missing clientId", input: `{"opcode":0,"emitter":0,"metadata":{"clientIPC":"/client"},"payload":{}}`, wantErr: ErrMissingClientID},
		{name: "missing clientIPC", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":"client"},"payload":{}}`, wantErr: ErrMissingClientIPC},
		{name: "missing payload", input: `{"opcode":0,"emitter":1,"metadata":{}}`, wantErr: ErrMissingPayload},
		{name: "null payload", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":null}`, wantErr: ErrMissingPayload},
		{name: "array payload", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":[]}`},
		{name: "unknown payload field", input: `{"opcode":0,"emitter":1,"metadata":{},"payload":{"extra":true}}`},
		{name: "missing raw", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"message":{"key":"k","value":"v"}}}`, wantErr: ErrMissingRaw},
		{name: "missing message", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":false}}`, wantErr: ErrMissingMessage},
		{name: "missing message key", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":true,"message":{"value":"v"}}}`, wantErr: ErrMissingMessageKey},
		{name: "missing message value", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":true,"message":{"key":"k"}}}`, wantErr: ErrMissingMessageValue},
		{name: "unknown message field", input: `{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":true,"message":{"key":"k","value":"v","extra":true}}}`},
		{
			name:    "oversized message",
			input:   fmt.Sprintf(`{"opcode":6,"emitter":1,"metadata":{},"payload":{"raw":true,"message":{"key":"k","value":%q}}}`, strings.Repeat("v", 1024)),
			wantErr: ErrMessageTooLarge,
		},
		{name: "additional object", input: validFrame + `{}`, wantErr: ErrAdditionalJSONValue},
		{name: "additional null", input: validFrame + ` null`, wantErr: ErrAdditionalJSONValue},
		{name: "additional scalar", input: validFrame + ` true`, wantErr: ErrAdditionalJSONValue},
		{name: "truncated trailing JSON", input: validFrame + ` {`},
		{name: "trailing garbage", input: validFrame + ` garbage`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decode([]byte(tt.input))
			if err == nil {
				t.Errorf("Decode(%q) error = nil, want an error", tt.input)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Decode(%q) error = %v, want %v", tt.input, err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, want %#v", tt.input, got, Data{})
			}
		})
	}
}
