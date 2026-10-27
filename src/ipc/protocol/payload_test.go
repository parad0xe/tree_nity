package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestNewPayloadJSON checks opcode dispatch, conversion, and strict field decoding.
func TestNewPayloadJSON(t *testing.T) {
	tests := []struct {
		name   string
		opcode Opcode
		input  string
		want   Payload
	}{
		{name: "list topic", opcode: ListTopic, input: `{}`, want: &ListTopicPayload{}},
		{name: "create topic", opcode: CreateTopic, input: `{}`, want: &CreateTopicPayload{}},
		{name: "consumer info", opcode: ConsumerInfo, input: `{}`, want: &ConsumerInfoPayload{}},
		{name: "topic subscription", opcode: TopicSubscription, input: `{}`, want: &TopicSubscriptionPayload{}},
		{name: "acknowledgement", opcode: Acknowledgement, input: `{}`, want: &AcknowledgementPayload{}},
		{name: "check topic", opcode: CheckTopic, input: `{}`, want: &CheckTopicPayload{}},
		{
			name:   "produce message",
			opcode: ProduceMessage,
			input:  `{"raw":true,"message":{"key":"key","value":"value"}}`,
			want:   &ProduceMessagePayload{raw: true, message: Message{key: "key", value: "value"}},
		},
		{
			name:   "produce non-raw message",
			opcode: ProduceMessage,
			input:  `{"raw":false,"message":{"key":"","value":""}}`,
			want:   &ProduceMessagePayload{raw: false, message: Message{}},
		},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, input: `{}`, want: &ConsumerDisconnectPayload{}},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, input: `{}`, want: &ConsumerHeartbeatPayload{}},
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
		name    string
		opcode  Opcode
		input   string
		wantErr error
	}{
		{name: "negative opcode", opcode: -1, input: `{}`, wantErr: ErrUnknownOpcode},
		{name: "unknown opcode", opcode: 9, input: `{}`, wantErr: ErrUnknownOpcode},
		{name: "empty input", opcode: ListTopic},
		{name: "whitespace", opcode: ListTopic, input: " \n\t"},
		{name: "malformed JSON", opcode: ListTopic, input: `{`},
		{name: "array", opcode: ListTopic, input: `[]`},
		{name: "string", opcode: ListTopic, input: `"payload"`},
		{name: "number", opcode: ListTopic, input: `1`},
		{name: "boolean", opcode: ListTopic, input: `false`},
		{name: "unknown topic field", opcode: CreateTopic, input: `{"topic":1}`},
		{name: "unknown create field", opcode: CreateTopic, input: `{"topic":"orders","extra":true}`},
		{name: "raw has wrong type", opcode: ProduceMessage, input: `{"raw":"true","message":{"key":"k","value":"v"}}`},
		{name: "message has wrong type", opcode: ProduceMessage, input: `{"raw":true,"message":[]}`},
		{name: "key has wrong type", opcode: ProduceMessage, input: `{"raw":true,"message":{"key":1,"value":"v"}}`},
		{name: "value has wrong type", opcode: ProduceMessage, input: `{"raw":true,"message":{"key":"k","value":false}}`},
		{name: "unknown message field", opcode: ProduceMessage, input: `{"raw":true,"message":{"key":"k","value":"v","offset":1}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newPayloadJSON(tt.opcode, json.RawMessage(tt.input))
			if err == nil || got != nil {
				t.Errorf("newPayloadJSON(%d, %q) = %#v, error = %v, want nil and an error", tt.opcode, tt.input, got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("newPayloadJSON(%d, %q) error = %v, want %v", tt.opcode, tt.input, err, tt.wantErr)
			}
		})
	}
}

// TestCreateTopicPayloadJSONRejectsFields keeps topic creation payloads empty.
func TestCreateTopicPayloadJSONRejectsFields(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "empty topic", input: `{"topic":""}`},
		{name: "named topic", input: `{"topic":"orders"}`},
		{name: "null topic", input: `{"topic":null}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newPayloadJSON(CreateTopic, json.RawMessage(tt.input))
			if err == nil || got != nil {
				t.Errorf("newPayloadJSON(%d, %s) = %#v, error = %v, want nil and an unknown field error", CreateTopic, tt.input, got, err)
			}
		})
	}
}

// TestProduceMessagePayloadJSONToPayload checks required fields and message errors.
func TestProduceMessagePayloadJSONToPayload(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "missing fields", input: `{}`, wantErr: ErrMissingRaw},
		{name: "missing raw", input: `{"message":{"key":"k","value":"v"}}`, wantErr: ErrMissingRaw},
		{name: "null raw", input: `{"raw":null,"message":{"key":"k","value":"v"}}`, wantErr: ErrMissingRaw},
		{name: "missing message", input: `{"raw":false}`, wantErr: ErrMissingMessage},
		{name: "null message", input: `{"raw":true,"message":null}`, wantErr: ErrMissingMessage},
		{name: "empty message", input: `{"raw":true,"message":{}}`, wantErr: ErrMissingMessageKey},
		{name: "missing message key", input: `{"raw":true,"message":{"value":"v"}}`, wantErr: ErrMissingMessageKey},
		{name: "missing message value", input: `{"raw":true,"message":{"key":"k"}}`, wantErr: ErrMissingMessageValue},
		{
			name:    "oversized message",
			input:   fmt.Sprintf(`{"raw":true,"message":{"key":"k","value":%q}}`, strings.Repeat("v", 1024)),
			wantErr: ErrMessageTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := newPayloadJSON(ProduceMessage, json.RawMessage(tt.input))
			if err != nil {
				t.Errorf("newPayloadJSON(%d, %s) error = %v, want nil", ProduceMessage, tt.input, err)
				return
			}
			if decoded == nil {
				t.Errorf("newPayloadJSON(%d, %s) = nil, want a payload decoder", ProduceMessage, tt.input)
				return
			}

			got, err := decoded.toPayload()
			if !errors.Is(err, tt.wantErr) || got != nil {
				t.Errorf("toPayload(%s) = %#v, error = %v, want nil and %v", tt.input, got, err, tt.wantErr)
			}
		})
	}
}
