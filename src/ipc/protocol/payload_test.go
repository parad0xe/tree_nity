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
		{name: "consumer info", opcode: ConsumerInfo, input: `{"clientId":"client-1","subscriber":"consumer-1"}`, want: &ConsumerInfoPayload{clientID: "client-1", subscriber: "consumer-1"}},
		{name: "topic subscription", opcode: TopicSubscription, input: `{"clientId":"client-1","topic":"orders","prefix":""}`, want: &TopicSubscriptionPayload{clientID: "client-1", topic: "orders"}},
		{name: "acknowledgement", opcode: Acknowledgement, input: `{"clientId":"client-1","offset":0}`, want: &AcknowledgementPayload{clientID: "client-1"}},
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
		{name: "consumer disconnect", opcode: ConsumerDisconnect, input: `{"clientId":"client-1"}`, want: &ConsumerDisconnectPayload{clientID: "client-1"}},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, input: `{"clientId":"client-1"}`, want: &ConsumerHeartbeatPayload{clientID: "client-1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			emitter := Client
			metadata := json.RawMessage(`{"clientIPC":"/client-1"}`)
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
		input  string
	}{
		{name: "list topic", opcode: ListTopic, input: `{"unexpected":true}`},
		{name: "create topic", opcode: CreateTopic, input: `{"topic":"orders","unexpected":true}`},
		{name: "consumer info", opcode: ConsumerInfo, input: `{"clientId":"client-1","subscriber":"consumer-1","unexpected":true}`},
		{name: "topic subscription", opcode: TopicSubscription, input: `{"clientId":"client-1","topic":"orders","prefix":"","unexpected":true}`},
		{name: "acknowledgement", opcode: Acknowledgement, input: `{"clientId":"client-1","offset":0,"unexpected":true}`},
		{name: "check topic", opcode: CheckTopic, input: `{"topic":"orders","unexpected":true}`},
		{name: "produce message", opcode: ProduceMessage, input: `{"topic":"orders","message":{"key":"aw==","value":"dg=="},"unexpected":true}`},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, input: `{"clientId":"client-1","unexpected":true}`},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, input: `{"clientId":"client-1","unexpected":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			emitter := Client
			metadata := json.RawMessage(`{"clientIPC":"/client-1"}`)
			payload := json.RawMessage(tt.input)
			input := dataJSON{Opcode: &tt.opcode, Emitter: &emitter, Metadata: &metadata, Payload: &payload}
			got, err := input.toData()
			if !errors.Is(err, ErrUnknownField) || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("toData(opcode=%d, emitter=%d, payload=%s) = %#v, error = %v, want zero Data and %v", tt.opcode, emitter, payload, got, err, ErrUnknownField)
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
		{name: "missing consumer client", emitter: Server, opcode: ConsumerInfoReply, wantErr: ErrMissingClient},
		{name: "missing subscription topic", emitter: Client, opcode: TopicSubscription, wantErr: ErrMissingTopic},
		{name: "missing error kind", emitter: Server, opcode: ErrorReply, wantErr: ErrMissingErrorKind},
		{name: "missing delivery message", emitter: Server, opcode: MessageDelivery, wantErr: ErrMissingMessage},
		{name: "server consumer heartbeat", emitter: Server, opcode: ConsumerHeartbeat, wantErr: ErrUnknownOpcode},
		{name: "negative server opcode", emitter: Server, opcode: -1, wantErr: ErrUnknownOpcode},
		{name: "unknown server opcode", emitter: Server, opcode: 9, wantErr: ErrUnknownOpcode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := json.RawMessage(`{}`)
			if tt.emitter == Client {
				metadata = json.RawMessage(`{"clientIPC":"/client-1"}`)
			}
			payload := json.RawMessage(`{}`)
			if tt.emitter == Client && tt.opcode == TopicSubscription {
				payload = json.RawMessage(`{"clientId":"client-1"}`)
			}
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
			metadata := json.RawMessage(`{"clientIPC":"/client-1"}`)
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
			metadata := json.RawMessage(`{"clientIPC":"/client-1"}`)
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
			frame := `{"opcode":5,"emitter":0,"metadata":{"clientIPC":"/client-1"},"payload":` + tt.payload + `}`
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
	tests := []struct {
		name    string
		emitter Emitter
		opcode  Opcode
		payload *TopicSubscriptionPayload
		wantErr error
	}{
		{name: "nil payload", emitter: Client, opcode: TopicSubscription, wantErr: ErrMissingPayload},
		{name: "zero value", emitter: Client, opcode: TopicSubscription, payload: &TopicSubscriptionPayload{}, wantErr: ErrInvalidPayload},
		{name: "long topic", emitter: Client, opcode: TopicSubscription, payload: &TopicSubscriptionPayload{clientID: "client-1", topic: strings.Repeat("t", 33)}, wantErr: ErrInvalidPayload},
		{name: "invalid topic", emitter: Client, opcode: TopicSubscription, payload: &TopicSubscriptionPayload{clientID: "client-1", topic: "user/events"}, wantErr: ErrInvalidPayload},
		{name: "invalid UTF-8 prefix", emitter: Client, opcode: TopicSubscription, payload: &TopicSubscriptionPayload{clientID: "client-1", topic: "orders", prefix: "\xff"}, wantErr: ErrInvalidPayload},
		{name: "wrong opcode", emitter: Client, opcode: CreateTopic, payload: &TopicSubscriptionPayload{clientID: "client-1", topic: "orders"}, wantErr: ErrInvalidPayload},
		{name: "server emitter", emitter: Server, opcode: TopicSubscriptionReply, payload: &TopicSubscriptionPayload{clientID: "client-1", topic: "orders"}, wantErr: ErrInvalidPayload},
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

// TestDecodeRejectsInvalidTopicSubscription checks required fields, types, and uint32 bounds.
func TestDecodeRejectsInvalidTopicSubscription(t *testing.T) {
	tests := []struct {
		name          string
		payload       string
		wantErr       error
		wantTypeError bool
	}{
		{name: "missing topic and prefix", payload: `{"clientId":"client-1"}`, wantErr: ErrMissingTopic},
		{name: "missing topic", payload: `{"clientId":"client-1","prefix":""}`, wantErr: ErrMissingTopic},
		{name: "null topic", payload: `{"clientId":"client-1","topic":null,"prefix":""}`, wantErr: ErrMissingTopic},
		{name: "empty topic", payload: `{"clientId":"client-1","topic":"","prefix":""}`, wantErr: ErrInvalidPayload},
		{name: "long topic", payload: `{"clientId":"client-1","topic":"` + strings.Repeat("t", 33) + `","prefix":""}`, wantErr: ErrInvalidPayload},
		{name: "invalid topic", payload: `{"clientId":"client-1","topic":"user/events","prefix":""}`, wantErr: ErrInvalidPayload},
		{name: "non-ASCII topic", payload: `{"clientId":"client-1","topic":"événements","prefix":""}`, wantErr: ErrInvalidPayload},
		{name: "numeric topic", payload: `{"clientId":"client-1","topic":1,"prefix":""}`},
		{name: "missing prefix", payload: `{"clientId":"client-1","topic":"orders"}`, wantErr: ErrMissingPrefix},
		{name: "null prefix", payload: `{"clientId":"client-1","topic":"orders","prefix":null}`, wantErr: ErrMissingPrefix},
		{name: "numeric prefix", payload: `{"clientId":"client-1","topic":"orders","prefix":1}`},
		{name: "boolean prefix", payload: `{"clientId":"client-1","topic":"orders","prefix":false}`},
		{name: "array prefix", payload: `{"clientId":"client-1","topic":"orders","prefix":[]}`},
		{name: "object prefix", payload: `{"clientId":"client-1","topic":"orders","prefix":{}}`},
		{name: "negative offset", payload: `{"clientId":"client-1","topic":"orders","prefix":"","offset":-1}`, wantTypeError: true},
		{name: "large negative offset", payload: `{"clientId":"client-1","topic":"orders","prefix":"","offset":-2147483648}`, wantTypeError: true},
		{name: "overflow offset", payload: `{"clientId":"client-1","topic":"orders","prefix":"","offset":4294967296}`, wantTypeError: true},
		{name: "underflow offset", payload: `{"clientId":"client-1","topic":"orders","prefix":"","offset":-2147483649}`, wantTypeError: true},
		{name: "fractional offset", payload: `{"clientId":"client-1","topic":"orders","prefix":"","offset":0.5}`},
		{name: "string offset", payload: `{"clientId":"client-1","topic":"orders","prefix":"","offset":"0"}`},
		{name: "boolean offset", payload: `{"clientId":"client-1","topic":"orders","prefix":"","offset":false}`},
		{name: "array offset", payload: `{"clientId":"client-1","topic":"orders","prefix":"","offset":[]}`},
		{name: "object offset", payload: `{"clientId":"client-1","topic":"orders","prefix":"","offset":{}}`},
		{name: "unknown field", payload: `{"clientId":"client-1","topic":"orders","prefix":"","extra":true}`},
		{name: "null payload", payload: `null`, wantErr: ErrMissingPayload},
		{name: "array payload", payload: `[]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := `{"opcode":3,"emitter":0,"metadata":{"clientIPC":"/client-1"},"payload":` + tt.payload + `}`
			got, err := Decode([]byte(frame))
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and an error", frame, got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Decode(%q) error = %v, want %v", frame, err, tt.wantErr)
			}
			if tt.wantTypeError {
				var typeErr *json.UnmarshalTypeError
				if !errors.As(err, &typeErr) || typeErr.Type != reflect.TypeFor[uint32]() {
					t.Errorf("Decode(%q) error = %v, want a JSON type error for uint32", frame, err)
				}
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
