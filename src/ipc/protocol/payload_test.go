package protocol

import (
	"encoding/base64"
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
		{name: "create topic", opcode: CreateTopic, input: `{"topic":"orders"}`, want: &CreateTopicPayload{topic: "orders"}},
		{name: "consumer info", opcode: ConsumerInfo, input: `{}`, want: &ConsumerInfoPayload{}},
		{name: "topic subscription", opcode: TopicSubscription, input: `{"topic":"orders","prefix":""}`, want: &TopicSubscriptionPayload{topic: "orders"}},
		{name: "acknowledgement", opcode: Acknowledgement, input: `{"offset":0}`, want: &AcknowledgementPayload{}},
		{name: "check topic", opcode: CheckTopic, input: `{"topic":"orders"}`, want: &CheckTopicPayload{topic: "orders"}},
		{
			name:   "produce message",
			opcode: ProduceMessage,
			input:  `{"topic":"orders","message":{"key":"a2V5","value":"dmFsdWU="}}`,
			want:   &ProduceMessagePayload{topic: "orders", message: Message{key: []byte("key"), value: []byte("value")}},
		},
		{
			name:   "produce empty message",
			opcode: ProduceMessage,
			input:  `{"topic":"orders","message":{"key":"","value":""}}`,
			want:   &ProduceMessagePayload{topic: "orders", message: Message{}},
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

// TestPayloadDecodingRejectsInvalidOperations checks opcode selection and required fields.
func TestPayloadDecodingRejectsInvalidOperations(t *testing.T) {
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		wantErr error
	}{
		{name: "negative emitter", emitter: -1, opcode: ListTopic, wantErr: ErrUnknownEmitter},
		{name: "unknown emitter", emitter: 2, opcode: ListTopic, wantErr: ErrUnknownEmitter},
		{name: "missing topics", emitter: Server, opcode: ListTopicReply, wantErr: ErrMissingTopics},
		{name: "server consumer info", emitter: Server, opcode: ConsumerInfo, wantErr: ErrUnknownOpcode},
		{name: "missing subscription topic", emitter: Client, opcode: TopicSubscription, wantErr: ErrMissingTopic},
		{name: "missing error kind", emitter: Server, opcode: ErrorReply, wantErr: ErrMissingErrorKind},
		{name: "missing delivery message", emitter: Server, opcode: MessageDelivery, wantErr: ErrMissingMessage},
		{name: "server consumer disconnect", emitter: Server, opcode: ConsumerDisconnect, wantErr: ErrUnknownOpcode},
		{name: "server consumer heartbeat", emitter: Server, opcode: ConsumerHeartbeat, wantErr: ErrUnknownOpcode},
		{name: "negative server opcode", emitter: Server, opcode: -1, wantErr: ErrUnknownOpcode},
		{name: "unknown server opcode", emitter: Server, opcode: 9, wantErr: ErrUnknownOpcode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := json.RawMessage(`{}`)
			if tt.emitter == Client {
				metadata = json.RawMessage(`{"clientId":"client-1","clientIPC":"/client-1"}`)
			}
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
		{name: "topic has wrong type", opcode: CreateTopic, input: `{"topic":1}`},
		{name: "unknown create field", opcode: CreateTopic, input: `{"topic":"orders","extra":true}`},
		{name: "legacy string raw flag", opcode: ProduceMessage, input: `{"topic":"orders","raw":"true","message":{"key":"aw==","value":"dg=="}}`},
		{name: "legacy true raw flag", opcode: ProduceMessage, input: `{"topic":"orders","raw":true,"message":{"key":"aw==","value":"dg=="}}`},
		{name: "legacy false raw flag", opcode: ProduceMessage, input: `{"topic":"orders","raw":false,"message":{"key":"aw==","value":"dg=="}}`},
		{name: "legacy null raw flag", opcode: ProduceMessage, input: `{"topic":"orders","raw":null,"message":{"key":"aw==","value":"dg=="}}`},
		{name: "message has wrong type", opcode: ProduceMessage, input: `{"topic":"orders","message":[]}`},
		{name: "key has wrong type", opcode: ProduceMessage, input: `{"topic":"orders","message":{"key":1,"value":"dg=="}}`},
		{name: "value has wrong type", opcode: ProduceMessage, input: `{"topic":"orders","message":{"key":"aw==","value":false}}`},
		{name: "unknown message field", opcode: ProduceMessage, input: `{"topic":"orders","message":{"key":"aw==","value":"dg==","offset":1}}`},
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

// TestCreateTopicPayloadJSONRejectsInvalidTopic checks required fields and name validation.
func TestCreateTopicPayloadJSONRejectsInvalidTopic(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "missing topic", input: `{}`, wantErr: ErrMissingTopic},
		{name: "empty topic", input: `{"topic":""}`, wantErr: ErrInvalidPayload},
		{name: "null topic", input: `{"topic":null}`, wantErr: ErrMissingTopic},
		{name: "oversized topic", input: `{"topic":"` + strings.Repeat("t", 33) + `"}`, wantErr: ErrInvalidPayload},
		{name: "space in topic", input: `{"topic":"user events"}`, wantErr: ErrInvalidPayload},
		{name: "slash in topic", input: `{"topic":"user/events"}`, wantErr: ErrInvalidPayload},
		{name: "non-ASCII topic", input: `{"topic":"événements"}`, wantErr: ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opcode := CreateTopic
			emitter := Client
			metadata := json.RawMessage(`{"clientId":"client-1","clientIPC":"/client-1"}`)
			payload := json.RawMessage(tt.input)
			input := dataJSON{Opcode: &opcode, Emitter: &emitter, Metadata: &metadata, Payload: &payload}
			got, err := input.toData()
			if !errors.Is(err, tt.wantErr) || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("toData(opcode=%d, emitter=%d, payload=%s) = %#v, error = %v, want zero Data and %v", opcode, emitter, payload, got, err, tt.wantErr)
			}
		})
	}
}

// TestCreateTopicReplyDecoding checks that an empty server response is now supported.
func TestCreateTopicReplyDecoding(t *testing.T) {
	emitter := Server
	opcode := CreateTopicReply
	metadata := json.RawMessage(`{}`)
	payload := json.RawMessage(`{}`)
	input := dataJSON{Opcode: &opcode, Emitter: &emitter, Metadata: &metadata, Payload: &payload}
	want := Data{opcode: opcode, emitter: emitter, metadata: &ServerMetadata{}, payload: &CreateTopicReplyPayload{}}
	got, err := input.toData()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("toData(opcode=%d, emitter=%d, payload=%s) = %#v, error = %v, want %#v and nil", opcode, emitter, payload, got, err, want)
	}
}

// TestCheckTopicPayloadJSONRejectsInvalidInput checks the required name and strict JSON fields.
func TestCheckTopicPayloadJSONRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "missing topic", payload: `{}`, wantErr: ErrMissingTopic},
		{name: "null topic", payload: `{"topic":null}`, wantErr: ErrMissingTopic},
		{name: "empty topic", payload: `{"topic":""}`, wantErr: ErrInvalidPayload},
		{name: "long topic", payload: `{"topic":"` + strings.Repeat("t", 33) + `"}`, wantErr: ErrInvalidPayload},
		{name: "invalid characters", payload: `{"topic":"user/events"}`, wantErr: ErrInvalidPayload},
		{name: "non-ASCII topic", payload: `{"topic":"événements"}`, wantErr: ErrInvalidPayload},
		{name: "numeric topic", payload: `{"topic":1}`},
		{name: "boolean topic", payload: `{"topic":false}`},
		{name: "array topic", payload: `{"topic":[]}`},
		{name: "unknown field", payload: `{"topic":"orders","extra":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := `{"opcode":5,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":` + tt.payload + `}`
			got, err := Decode([]byte(frame))
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and an error", frame, got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Decode(%q) error = %v, want %v", frame, err, tt.wantErr)
			}
		})
	}
}

// TestTopicSubscriptionPayloadToJSONRejectsInvalidInput checks content and routing validation.
func TestTopicSubscriptionPayloadToJSONRejectsInvalidInput(t *testing.T) {
	negative := int32(-1)
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *TopicSubscriptionPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Client, opcode: TopicSubscription, wantErr: ErrMissingPayload},
		{name: "zero value", emitter: Client, opcode: TopicSubscription, payload: &TopicSubscriptionPayload{}, wantErr: ErrInvalidPayload},
		{name: "long topic", emitter: Client, opcode: TopicSubscription, payload: &TopicSubscriptionPayload{topic: strings.Repeat("t", 33)}, wantErr: ErrInvalidPayload},
		{name: "invalid topic", emitter: Client, opcode: TopicSubscription, payload: &TopicSubscriptionPayload{topic: "user/events"}, wantErr: ErrInvalidPayload},
		{name: "invalid UTF-8 prefix", emitter: Client, opcode: TopicSubscription, payload: &TopicSubscriptionPayload{topic: "orders", prefix: "\xff"}, wantErr: ErrInvalidPayload},
		{name: "negative offset", emitter: Client, opcode: TopicSubscription, payload: &TopicSubscriptionPayload{topic: "orders", offset: &negative}, wantErr: ErrInvalidPayload},
		{name: "wrong opcode", emitter: Client, opcode: CreateTopic, payload: &TopicSubscriptionPayload{topic: "orders"}, wantErr: ErrInvalidPayload},
		{name: "server emitter", emitter: Server, opcode: TopicSubscriptionReply, payload: &TopicSubscriptionPayload{topic: "orders"}, wantErr: ErrInvalidPayload},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.payload.toJSON(tt.emitter, tt.opcode)
			if !errors.Is(err, tt.wantErr) || got != nil {
				t.Errorf("toJSON(emitter=%d, opcode=%d, payload=%#v) = %#v, error = %v, want nil and %v", tt.emitter, tt.opcode, tt.payload, got, err, tt.wantErr)
			}
		})
	}
}

// TestDecodeRejectsInvalidTopicSubscription checks required fields, types, and int32 bounds.
func TestDecodeRejectsInvalidTopicSubscription(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "empty object", payload: `{}`, wantErr: ErrMissingTopic},
		{name: "missing topic", payload: `{"prefix":""}`, wantErr: ErrMissingTopic},
		{name: "null topic", payload: `{"topic":null,"prefix":""}`, wantErr: ErrMissingTopic},
		{name: "empty topic", payload: `{"topic":"","prefix":""}`, wantErr: ErrInvalidPayload},
		{name: "long topic", payload: `{"topic":"` + strings.Repeat("t", 33) + `","prefix":""}`, wantErr: ErrInvalidPayload},
		{name: "invalid topic", payload: `{"topic":"user/events","prefix":""}`, wantErr: ErrInvalidPayload},
		{name: "non-ASCII topic", payload: `{"topic":"événements","prefix":""}`, wantErr: ErrInvalidPayload},
		{name: "numeric topic", payload: `{"topic":1,"prefix":""}`},
		{name: "missing prefix", payload: `{"topic":"orders"}`, wantErr: ErrMissingPrefix},
		{name: "null prefix", payload: `{"topic":"orders","prefix":null}`, wantErr: ErrMissingPrefix},
		{name: "numeric prefix", payload: `{"topic":"orders","prefix":1}`},
		{name: "boolean prefix", payload: `{"topic":"orders","prefix":false}`},
		{name: "array prefix", payload: `{"topic":"orders","prefix":[]}`},
		{name: "object prefix", payload: `{"topic":"orders","prefix":{}}`},
		{name: "negative offset", payload: `{"topic":"orders","prefix":"","offset":-1}`, wantErr: ErrInvalidPayload},
		{name: "minimum int32 offset", payload: `{"topic":"orders","prefix":"","offset":-2147483648}`, wantErr: ErrInvalidPayload},
		{name: "overflow offset", payload: `{"topic":"orders","prefix":"","offset":2147483648}`},
		{name: "underflow offset", payload: `{"topic":"orders","prefix":"","offset":-2147483649}`},
		{name: "fractional offset", payload: `{"topic":"orders","prefix":"","offset":0.5}`},
		{name: "string offset", payload: `{"topic":"orders","prefix":"","offset":"0"}`},
		{name: "boolean offset", payload: `{"topic":"orders","prefix":"","offset":false}`},
		{name: "array offset", payload: `{"topic":"orders","prefix":"","offset":[]}`},
		{name: "object offset", payload: `{"topic":"orders","prefix":"","offset":{}}`},
		{name: "unknown field", payload: `{"topic":"orders","prefix":"","extra":true}`},
		{name: "null payload", payload: `null`, wantErr: ErrMissingPayload},
		{name: "array payload", payload: `[]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := `{"opcode":3,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":` + tt.payload + `}`
			got, err := Decode([]byte(frame))
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and an error", frame, got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Decode(%q) error = %v, want %v", frame, err, tt.wantErr)
			}
		})
	}
}

// TestProduceMessagePayloadJSONToPayloadRejectsInvalidInput checks required fields and message errors.
func TestProduceMessagePayloadJSONToPayloadRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{name: "missing fields", input: `{}`, wantErr: ErrMissingTopic},
		{name: "missing topic", input: `{"message":{"key":"aw==","value":"dg=="}}`, wantErr: ErrMissingTopic},
		{name: "null topic", input: `{"topic":null,"message":{"key":"aw==","value":"dg=="}}`, wantErr: ErrMissingTopic},
		{name: "empty topic", input: `{"topic":"","message":{"key":"aw==","value":"dg=="}}`, wantErr: ErrInvalidPayload},
		{name: "long topic", input: `{"topic":"` + strings.Repeat("t", 33) + `","message":{"key":"aw==","value":"dg=="}}`, wantErr: ErrInvalidPayload},
		{name: "invalid topic", input: `{"topic":"user/events","message":{"key":"aw==","value":"dg=="}}`, wantErr: ErrInvalidPayload},
		{name: "missing message", input: `{"topic":"orders"}`, wantErr: ErrMissingMessage},
		{name: "null message", input: `{"topic":"orders","message":null}`, wantErr: ErrMissingMessage},
		{name: "empty message", input: `{"topic":"orders","message":{}}`, wantErr: ErrMissingMessageKey},
		{name: "missing message key", input: `{"topic":"orders","message":{"value":"dg=="}}`, wantErr: ErrMissingMessageKey},
		{name: "missing message value", input: `{"topic":"orders","message":{"key":"aw=="}}`, wantErr: ErrMissingMessageValue},
		{name: "invalid base64 key", input: `{"topic":"orders","message":{"key":"?","value":""}}`, wantErr: ErrInvalidPayload},
		{name: "invalid base64 value", input: `{"topic":"orders","message":{"key":"","value":"?"}}`, wantErr: ErrInvalidPayload},
		{
			name:    "oversized message",
			input:   fmt.Sprintf(`{"topic":"orders","message":{"key":"aw==","value":%q}}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("v", 1024)))),
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
