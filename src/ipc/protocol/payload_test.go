package protocol

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestNewPayloadJSON checks opcode dispatch, conversion, and strict field decoding.
func TestNewPayloadJSON(t *testing.T) {
	tests := []struct {
		name   string
		opcode op
		input  string
		want   Payload
	}{
		{name: "list topic", opcode: listTopic, input: `{}`, want: &ListTopicPayload{}},
		{name: "create topic", opcode: createTopic, input: `{}`, want: &CreateTopicPayload{}},
		{name: "consumer info", opcode: consumerInfo, input: `{}`, want: &ConsumerInfoPayload{}},
		{name: "topic subscription", opcode: topicSubscription, input: `{}`, want: &TopicSubscriptionPayload{}},
		{name: "acknowledgement", opcode: acknowledgement, input: `{}`, want: &AcknowledgementPayload{}},
		{name: "check topic", opcode: checkTopic, input: `{}`, want: &CheckTopicPayload{}},
		{
			name:   "produce message",
			opcode: produceMessage,
			input:  `{"raw":true,"message":{"key":"key","value":"value"}}`,
			want:   &ProduceMessagePayload{raw: true, message: Message{key: "key", value: "value"}},
		},
		{
			name:   "produce non-raw message",
			opcode: produceMessage,
			input:  `{"raw":false,"message":{"key":"","value":""}}`,
			want:   &ProduceMessagePayload{raw: false, message: Message{}},
		},
		{name: "consumer disconnect", opcode: consumerDisconnect, input: `{}`, want: &ConsumerDisconnectPayload{}},
		{name: "consumer heartbeat", opcode: consumerHeartbeat, input: `{}`, want: &ConsumerHeartbeatPayload{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := newPayloadJSON(tt.opcode, json.RawMessage(tt.input))
			if err != nil {
				t.Errorf("newPayloadJSON(%d, %s) error = %v, want nil", tt.opcode, tt.input, err)
				return
			}
			if decoded == nil {
				t.Errorf("newPayloadJSON(%d, %s) = nil, want a payload decoder", tt.opcode, tt.input)
				return
			}

			got, err := decoded.toPayload()
			if err != nil {
				t.Errorf("toPayload(opcode=%d, input=%s) error = %v, want nil", tt.opcode, tt.input, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toPayload(opcode=%d, input=%s) = %#v, want %#v", tt.opcode, tt.input, got, tt.want)
			}

			unknownField := json.RawMessage(`{"unexpected":true}`)
			if got, err := newPayloadJSON(tt.opcode, unknownField); err == nil || got != nil {
				t.Errorf("newPayloadJSON(%d, %s) = %#v, error = %v, want nil and an error", tt.opcode, unknownField, got, err)
			}
		})
	}
}

// TestNewPayloadJSONRejectsInvalidInput checks unsupported opcodes and JSON shapes.
func TestNewPayloadJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name   string
		opcode op
		input  string
	}{
		{name: "negative opcode", opcode: -1, input: `{}`},
		{name: "unknown opcode", opcode: 9, input: `{}`},
		{name: "empty input", opcode: listTopic},
		{name: "whitespace", opcode: listTopic, input: " \n\t"},
		{name: "malformed JSON", opcode: listTopic, input: `{`},
		{name: "array", opcode: listTopic, input: `[]`},
		{name: "string", opcode: listTopic, input: `"payload"`},
		{name: "number", opcode: listTopic, input: `1`},
		{name: "boolean", opcode: listTopic, input: `false`},
		{name: "raw has wrong type", opcode: produceMessage, input: `{"raw":"true","message":{"key":"k","value":"v"}}`},
		{name: "message has wrong type", opcode: produceMessage, input: `{"raw":true,"message":[]}`},
		{name: "key has wrong type", opcode: produceMessage, input: `{"raw":true,"message":{"key":1,"value":"v"}}`},
		{name: "value has wrong type", opcode: produceMessage, input: `{"raw":true,"message":{"key":"k","value":false}}`},
		{name: "unknown message field", opcode: produceMessage, input: `{"raw":true,"message":{"key":"k","value":"v","offset":1}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newPayloadJSON(tt.opcode, json.RawMessage(tt.input))
			if err == nil || got != nil {
				t.Errorf("newPayloadJSON(%d, %q) = %#v, error = %v, want nil and an error", tt.opcode, tt.input, got, err)
			}
		})
	}
}

// TestProduceMessagePayloadJSONToPayload checks required fields and message errors.
func TestProduceMessagePayloadJSONToPayload(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "missing fields", input: `{}`, wantErr: "missing raw"},
		{name: "missing raw", input: `{"message":{"key":"k","value":"v"}}`, wantErr: "missing raw"},
		{name: "null raw", input: `{"raw":null,"message":{"key":"k","value":"v"}}`, wantErr: "missing raw"},
		{name: "missing message", input: `{"raw":false}`, wantErr: "missing message"},
		{name: "null message", input: `{"raw":true,"message":null}`, wantErr: "missing message"},
		{name: "empty message", input: `{"raw":true,"message":{}}`, wantErr: "missing message key"},
		{name: "missing message key", input: `{"raw":true,"message":{"value":"v"}}`, wantErr: "missing message key"},
		{name: "missing message value", input: `{"raw":true,"message":{"key":"k"}}`, wantErr: "missing message value"},
		{
			name:    "oversized message",
			input:   fmt.Sprintf(`{"raw":true,"message":{"key":"k","value":%q}}`, strings.Repeat("v", 1024)),
			wantErr: "message exceeds 1024 bytes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := newPayloadJSON(produceMessage, json.RawMessage(tt.input))
			if err != nil {
				t.Errorf("newPayloadJSON(%d, %s) error = %v, want nil", produceMessage, tt.input, err)
				return
			}
			if decoded == nil {
				t.Errorf("newPayloadJSON(%d, %s) = nil, want a payload decoder", produceMessage, tt.input)
				return
			}

			got, err := decoded.toPayload()
			if err == nil || err.Error() != tt.wantErr || got != nil {
				t.Errorf("toPayload(%s) = %#v, error = %v, want nil and %q", tt.input, got, err, tt.wantErr)
			}
		})
	}
}
