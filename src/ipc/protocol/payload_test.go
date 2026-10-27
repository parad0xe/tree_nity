package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// TestClientPayloadDecoding checks client opcode dispatch and conversion.
func TestClientPayloadDecoding(t *testing.T) {
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
			emitter := Client
			metadata := json.RawMessage(`{"clientId":"client-1","clientIPC":"/client-1"}`)
			payload := json.RawMessage(tt.input)
			input := dataJSON{Opcode: &tt.opcode, Emitter: &emitter, Metadata: &metadata, Payload: &payload}
			got, err := input.toData()
			if err != nil || !reflect.DeepEqual(got.Payload(), tt.want) {
				t.Errorf("toData(opcode=%d, emitter=%d, payload=%s) payload = %#v, error = %v, want %#v and nil", tt.opcode, emitter, payload, got.Payload(), err, tt.want)
			}
		})
	}
}

// TestClientPayloadDecodingRejectsUnknownFields checks strict decoding for every client opcode.
func TestClientPayloadDecodingRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name   string
		opcode Opcode
	}{
		{name: "list topic", opcode: ListTopic},
		{name: "create topic", opcode: CreateTopic},
		{name: "consumer info", opcode: ConsumerInfo},
		{name: "topic subscription", opcode: TopicSubscription},
		{name: "acknowledgement", opcode: Acknowledgement},
		{name: "check topic", opcode: CheckTopic},
		{name: "produce message", opcode: ProduceMessage},
		{name: "consumer disconnect", opcode: ConsumerDisconnect},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			emitter := Client
			metadata := json.RawMessage(`{"clientId":"client-1","clientIPC":"/client-1"}`)
			payload := json.RawMessage(`{"unexpected":true}`)
			input := dataJSON{Opcode: &tt.opcode, Emitter: &emitter, Metadata: &metadata, Payload: &payload}
			got, err := input.toData()
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("toData(opcode=%d, emitter=%d, payload=%s) = %#v, error = %v, want zero Data and an unknown field error", tt.opcode, emitter, payload, got, err)
			}
		})
	}
}

// TestPayloadDecodingRejectsUnsupportedOperations checks emitter-specific opcode selection.
func TestPayloadDecodingRejectsUnsupportedOperations(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		wantErr error
	}{
		{name: "negative emitter", emitter: -1, opcode: ListTopic, wantErr: ErrUnknownEmitter},
		{name: "unknown emitter", emitter: 2, opcode: ListTopic, wantErr: ErrUnknownEmitter},
		{name: "server list topic", emitter: Server, opcode: ListTopic, wantErr: ErrUnknownOpcode},
		{name: "server create topic", emitter: Server, opcode: CreateTopic, wantErr: ErrUnknownOpcode},
		{name: "server consumer info", emitter: Server, opcode: ConsumerInfo, wantErr: ErrUnknownOpcode},
		{name: "server topic subscription", emitter: Server, opcode: TopicSubscription, wantErr: ErrUnknownOpcode},
		{name: "server acknowledgement", emitter: Server, opcode: Acknowledgement, wantErr: ErrUnknownOpcode},
		{name: "server check topic", emitter: Server, opcode: CheckTopic, wantErr: ErrUnknownOpcode},
		{name: "server produce message", emitter: Server, opcode: ProduceMessage, wantErr: ErrUnknownOpcode},
		{name: "server consumer disconnect", emitter: Server, opcode: ConsumerDisconnect, wantErr: ErrUnknownOpcode},
		{name: "server consumer heartbeat", emitter: Server, opcode: ConsumerHeartbeat, wantErr: ErrUnknownOpcode},
		{name: "negative server opcode", emitter: Server, opcode: -1, wantErr: ErrUnknownOpcode},
		{name: "unknown server opcode", emitter: Server, opcode: 9, wantErr: ErrUnknownOpcode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := json.RawMessage(`{}`)
			payload := json.RawMessage(`{}`)
			input := dataJSON{Opcode: &tt.opcode, Emitter: &tt.emitter, Metadata: &metadata, Payload: &payload}
			got, err := input.toData()
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("toData(opcode=%d, emitter=%d, payload=%s) = %#v, error = %v, want zero Data and %v", tt.opcode, tt.emitter, payload, got, err, tt.wantErr)
			}
		})
	}
}

// TestClientPayloadDecodingRejectsInvalidInput checks unsupported opcodes and JSON shapes.
func TestClientPayloadDecodingRejectsInvalidInput(t *testing.T) {
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
			emitter := Client
			metadata := json.RawMessage(`{"clientId":"client-1","clientIPC":"/client-1"}`)
			payload := json.RawMessage(tt.input)
			input := dataJSON{Opcode: &tt.opcode, Emitter: &emitter, Metadata: &metadata, Payload: &payload}
			got, err := input.toData()
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("toData(opcode=%d, emitter=%d, payload=%q) = %#v, error = %v, want zero Data and an error", tt.opcode, emitter, payload, got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("toData(opcode=%d, emitter=%d, payload=%q) error = %v, want %v", tt.opcode, emitter, payload, err, tt.wantErr)
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
			opcode := CreateTopic
			emitter := Client
			metadata := json.RawMessage(`{"clientId":"client-1","clientIPC":"/client-1"}`)
			payload := json.RawMessage(tt.input)
			input := dataJSON{Opcode: &opcode, Emitter: &emitter, Metadata: &metadata, Payload: &payload}
			got, err := input.toData()
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("toData(opcode=%d, emitter=%d, payload=%s) = %#v, error = %v, want zero Data and an unknown field error", opcode, emitter, payload, got, err)
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
			var decoded produceMessagePayloadJSON
			if err := json.Unmarshal([]byte(tt.input), &decoded); err != nil {
				t.Errorf("Unmarshal(%q) error = %v, want nil", tt.input, err)
				return
			}

			got, err := decoded.toPayload()
			if !errors.Is(err, tt.wantErr) || got != nil {
				t.Errorf("toPayload(%s) = %#v, error = %v, want nil and %v", tt.input, got, err, tt.wantErr)
			}
		})
	}
}
