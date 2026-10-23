package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type failingMetadata struct {
	err error
}

func (m *failingMetadata) toJSON(_ emitter) (metadataJSON, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	return nil, m.err
}

func (m *failingMetadata) validate() error {
	if m == nil {
		return ErrMissingMetadata
	}
	return nil
}

type failingPayload struct {
	err error
}

func (p *failingPayload) toJSON(_ Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return nil, p.err
}

func (p *failingPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

// TestEncode checks the wire representation and round trips for every opcode and emitter.
func TestEncode(t *testing.T) {
	payloads := []struct {
		name    string
		opcode  Opcode
		payload Payload
		json    string
	}{
		{name: "list topic", opcode: ListTopic, payload: &ListTopicPayload{}, json: `{}`},
		{name: "create topic", opcode: CreateTopic, payload: &CreateTopicPayload{}, json: `{}`},
		{name: "consumer info", opcode: ConsumerInfo, payload: &ConsumerInfoPayload{}, json: `{}`},
		{name: "topic subscription", opcode: TopicSubscription, payload: &TopicSubscriptionPayload{}, json: `{}`},
		{name: "acknowledgement", opcode: Acknowledgement, payload: &AcknowledgementPayload{}, json: `{}`},
		{name: "check topic", opcode: CheckTopic, payload: &CheckTopicPayload{}, json: `{}`},
		{
			name:    "produce raw message",
			opcode:  ProduceMessage,
			payload: &ProduceMessagePayload{raw: true, message: Message{key: "key", value: "value"}},
			json:    `{"raw":true,"message":{"key":"key","value":"value"}}`,
		},
		{
			name:    "produce empty non-raw message",
			opcode:  ProduceMessage,
			payload: &ProduceMessagePayload{},
			json:    `{"raw":false,"message":{"key":"","value":""}}`,
		},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, payload: &ConsumerDisconnectPayload{}, json: `{}`},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, payload: &ConsumerHeartbeatPayload{}, json: `{}`},
	}
	emitters := []struct {
		name     string
		emitter  emitter
		metadata Metadata
		json     string
	}{
		{
			name:     "client",
			emitter:  client,
			metadata: &ClientMetadata{clientId: "client-1", clientIPC: "/client-1"},
			json:     `{"clientId":"client-1","clientIPC":"/client-1"}`,
		},
		{name: "server", emitter: server, metadata: &ServerMetadata{}, json: `{}`},
	}

	for _, p := range payloads {
		for _, e := range emitters {
			t.Run(p.name+"/"+e.name, func(t *testing.T) {
				data := Data{opcode: p.opcode, emitter: e.emitter, metadata: e.metadata, payload: p.payload}
				frame, err := Encode(data)
				if err != nil {
					t.Errorf("Encode(%#v) error = %v, want nil", data, err)
					return
				}

				wantFrame := fmt.Sprintf(`{"opcode":%d,"emitter":%d,"metadata":%s,"payload":%s}`, p.opcode, e.emitter, e.json, p.json)
				var gotJSON, wantJSON any
				if err := json.Unmarshal(frame, &gotJSON); err != nil {
					t.Errorf("Unmarshal(Encode(%#v)) with frame %q error = %v, want nil", data, frame, err)
					return
				}
				if err := json.Unmarshal([]byte(wantFrame), &wantJSON); err != nil {
					t.Errorf("Unmarshal(%q) for expected frame error = %v, want nil", wantFrame, err)
					return
				}
				if !reflect.DeepEqual(gotJSON, wantJSON) {
					t.Errorf("Encode(%#v) = %s, want JSON equivalent to %s", data, frame, wantFrame)
				}

				got, err := Decode(frame)
				if err != nil {
					t.Errorf("Decode(Encode(%#v)) with frame %q error = %v, want nil", data, frame, err)
					return
				}
				if !reflect.DeepEqual(got, data) {
					t.Errorf("Decode(Encode(%#v)) = %#v, want %#v", data, got, data)
				}
			})
		}
	}
}

// TestEncodeStringsAndLimits checks escaping, byte boundaries, and the internal offset.
func TestEncodeStringsAndLimits(t *testing.T) {
	tests := []struct {
		name     string
		metadata ClientMetadata
		message  Message
	}{
		{name: "empty strings"},
		{name: "minimal strings", metadata: ClientMetadata{clientId: "c", clientIPC: "q"}, message: Message{key: "k", value: "v"}},
		{
			name:     "escaped strings",
			metadata: ClientMetadata{clientId: "client\"\\\n\x00", clientIPC: "/é/猫<&>\u2028"},
			message:  Message{key: "key\"\\\n\t\x00", value: "é/猫<&>\u2029"},
		},
		{name: "key at byte limit", message: Message{key: strings.Repeat("k", MessageSize)}},
		{name: "value at byte limit", message: Message{value: strings.Repeat("v", MessageSize)}},
		{name: "combined byte limit", message: Message{key: strings.Repeat("k", MessageSize/2), value: strings.Repeat("v", MessageSize/2)}},
		{name: "UTF-8 at byte limit", message: Message{value: strings.Repeat("é", MessageSize/2)}},
		{name: "JSON escaping at byte limit", message: Message{value: strings.Repeat("\x00", MessageSize)}},
		{
			name:     "large metadata",
			metadata: ClientMetadata{clientId: strings.Repeat("c", 4096), clientIPC: strings.Repeat("q", 4096)},
		},
		{name: "internal offset", message: Message{offset: 42, key: "key", value: "value"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := Data{opcode: ProduceMessage, emitter: client, metadata: &tt.metadata, payload: &ProduceMessagePayload{message: tt.message}}
			frame, err := Encode(data)
			if err != nil {
				t.Errorf("Encode(%#v) with message %+v error = %v, want nil", data, tt.message, err)
				return
			}
			got, err := Decode(frame)
			if err != nil {
				t.Errorf("Decode(Encode(%#v)) with frame %q error = %v, want nil", data, frame, err)
				return
			}
			want := Data{
				opcode:   ProduceMessage,
				emitter:  client,
				metadata: &tt.metadata,
				payload:  &ProduceMessagePayload{message: Message{key: tt.message.key, value: tt.message.value}},
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Decode(Encode(%#v)) with message %+v = %#v, want %#v", data, tt.message, got, want)
			}
		})
	}
}

// TestEncodeRejectsInvalidInput checks missing fields, unknown codes, types, and message limits.
func TestEncodeRejectsInvalidInput(t *testing.T) {
	conversionErr := errors.New("conversion failed")
	tests := []struct {
		name    string
		data    Data
		wantErr error
	}{
		{name: "zero value", wantErr: ErrMissingMetadata},
		{name: "negative emitter", data: Data{emitter: -1, metadata: &ServerMetadata{}, payload: &ListTopicPayload{}}, wantErr: ErrUnknownEmitter},
		{name: "unknown emitter", data: Data{emitter: 2, metadata: &ServerMetadata{}, payload: &ListTopicPayload{}}, wantErr: ErrUnknownEmitter},
		{name: "negative opcode", data: Data{opcode: -1, emitter: server, metadata: &ServerMetadata{}, payload: &ListTopicPayload{}}, wantErr: ErrUnknownOpcode},
		{name: "unknown opcode", data: Data{opcode: 9, emitter: server, metadata: &ServerMetadata{}, payload: &ListTopicPayload{}}, wantErr: ErrUnknownOpcode},
		{name: "missing client metadata", data: Data{emitter: client, payload: &ListTopicPayload{}}, wantErr: ErrMissingMetadata},
		{name: "missing server metadata", data: Data{emitter: server, payload: &ListTopicPayload{}}, wantErr: ErrMissingMetadata},
		{name: "nil client metadata pointer", data: Data{emitter: client, metadata: (*ClientMetadata)(nil), payload: &ListTopicPayload{}}, wantErr: ErrMissingMetadata},
		{name: "nil server metadata pointer", data: Data{emitter: server, metadata: (*ServerMetadata)(nil), payload: &ListTopicPayload{}}, wantErr: ErrMissingMetadata},
		{name: "server metadata from client", data: Data{emitter: client, metadata: &ServerMetadata{}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "client metadata from server", data: Data{emitter: server, metadata: &ClientMetadata{}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "metadata conversion error", data: Data{emitter: server, metadata: &failingMetadata{err: ErrInvalidMetadata}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "wrapped metadata conversion error", data: Data{emitter: server, metadata: &failingMetadata{err: fmt.Errorf("metadata: %w", conversionErr)}, payload: &ListTopicPayload{}}, wantErr: conversionErr},
		{name: "invalid UTF-8 client ID", data: Data{emitter: client, metadata: &ClientMetadata{clientId: "\xff"}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "invalid UTF-8 client IPC", data: Data{emitter: client, metadata: &ClientMetadata{clientIPC: "\xff"}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "missing payload", data: Data{emitter: server, metadata: &ServerMetadata{}}, wantErr: ErrMissingPayload},
		{name: "payload conversion error", data: Data{emitter: server, metadata: &ServerMetadata{}, payload: &failingPayload{err: ErrInvalidPayload}}, wantErr: ErrInvalidPayload},
		{name: "wrapped payload conversion error", data: Data{emitter: server, metadata: &ServerMetadata{}, payload: &failingPayload{err: fmt.Errorf("payload: %w", conversionErr)}}, wantErr: conversionErr},
		{
			name:    "oversized key",
			data:    Data{opcode: ProduceMessage, emitter: server, metadata: &ServerMetadata{}, payload: &ProduceMessagePayload{message: Message{key: strings.Repeat("k", MessageSize+1)}}},
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "oversized value",
			data:    Data{opcode: ProduceMessage, emitter: server, metadata: &ServerMetadata{}, payload: &ProduceMessagePayload{message: Message{value: strings.Repeat("v", MessageSize+1)}}},
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "combined size exceeds limit",
			data:    Data{opcode: ProduceMessage, emitter: server, metadata: &ServerMetadata{}, payload: &ProduceMessagePayload{message: Message{key: strings.Repeat("k", MessageSize/2), value: strings.Repeat("v", MessageSize/2+1)}}},
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "UTF-8 exceeds byte limit",
			data:    Data{opcode: ProduceMessage, emitter: server, metadata: &ServerMetadata{}, payload: &ProduceMessagePayload{message: Message{key: "k", value: strings.Repeat("é", MessageSize/2)}}},
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "invalid UTF-8 message key",
			data:    Data{opcode: ProduceMessage, emitter: server, metadata: &ServerMetadata{}, payload: &ProduceMessagePayload{message: Message{key: "\xff"}}},
			wantErr: ErrInvalidPayload,
		},
		{
			name:    "invalid UTF-8 message value",
			data:    Data{opcode: ProduceMessage, emitter: server, metadata: &ServerMetadata{}, payload: &ProduceMessagePayload{message: Message{value: "\xff"}}},
			wantErr: ErrInvalidPayload,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := Encode(tt.data)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Encode(%#v) error = %v, want %v", tt.data, err, tt.wantErr)
			}
			if frame != nil {
				t.Errorf("Encode(%#v) frame = %q, want nil on error", tt.data, frame)
			}
		})
	}
}

// TestEncodeRejectsIncompatiblePayloads checks opcode mismatches and nil payload pointers.
func TestEncodeRejectsIncompatiblePayloads(t *testing.T) {
	payloads := []struct {
		name    string
		opcode  Opcode
		payload Payload
		nilPtr  Payload
	}{
		{name: "list topic", opcode: ListTopic, payload: &ListTopicPayload{}, nilPtr: (*ListTopicPayload)(nil)},
		{name: "create topic", opcode: CreateTopic, payload: &CreateTopicPayload{}, nilPtr: (*CreateTopicPayload)(nil)},
		{name: "consumer info", opcode: ConsumerInfo, payload: &ConsumerInfoPayload{}, nilPtr: (*ConsumerInfoPayload)(nil)},
		{name: "topic subscription", opcode: TopicSubscription, payload: &TopicSubscriptionPayload{}, nilPtr: (*TopicSubscriptionPayload)(nil)},
		{name: "acknowledgement", opcode: Acknowledgement, payload: &AcknowledgementPayload{}, nilPtr: (*AcknowledgementPayload)(nil)},
		{name: "check topic", opcode: CheckTopic, payload: &CheckTopicPayload{}, nilPtr: (*CheckTopicPayload)(nil)},
		{name: "produce message", opcode: ProduceMessage, payload: &ProduceMessagePayload{}, nilPtr: (*ProduceMessagePayload)(nil)},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, payload: &ConsumerDisconnectPayload{}, nilPtr: (*ConsumerDisconnectPayload)(nil)},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, payload: &ConsumerHeartbeatPayload{}, nilPtr: (*ConsumerHeartbeatPayload)(nil)},
	}
	for _, operation := range payloads {
		t.Run(operation.name+"/nil pointer", func(t *testing.T) {
			data := Data{opcode: operation.opcode, emitter: server, metadata: &ServerMetadata{}, payload: operation.nilPtr}
			frame, err := Encode(data)
			if !errors.Is(err, ErrMissingPayload) || frame != nil {
				t.Errorf("Encode(%#v) = %q, error = %v, want nil frame and %v", data, frame, err, ErrMissingPayload)
			}
		})
		for _, p := range payloads {
			if p.opcode == operation.opcode {
				continue
			}
			t.Run(operation.name+"/"+p.name, func(t *testing.T) {
				data := Data{opcode: operation.opcode, emitter: server, metadata: &ServerMetadata{}, payload: p.payload}
				frame, err := Encode(data)
				if !errors.Is(err, ErrInvalidPayload) || frame != nil {
					t.Errorf("Encode(%#v) = %q, error = %v, want nil frame and %v", data, frame, err, ErrInvalidPayload)
				}
			})
		}
	}
}

// TestDecode checks every opcode for client and server frames.
func TestDecode(t *testing.T) {
	payloads := []struct {
		name    string
		opcode  Opcode
		payload string
		want    Payload
	}{
		{name: "list topic", opcode: ListTopic, payload: `{}`, want: &ListTopicPayload{}},
		{name: "create topic", opcode: CreateTopic, payload: `{}`, want: &CreateTopicPayload{}},
		{name: "consumer info", opcode: ConsumerInfo, payload: `{}`, want: &ConsumerInfoPayload{}},
		{name: "topic subscription", opcode: TopicSubscription, payload: `{}`, want: &TopicSubscriptionPayload{}},
		{name: "acknowledgement", opcode: Acknowledgement, payload: `{}`, want: &AcknowledgementPayload{}},
		{name: "check topic", opcode: CheckTopic, payload: `{}`, want: &CheckTopicPayload{}},
		{
			name:    "produce raw message",
			opcode:  ProduceMessage,
			payload: `{"raw":true,"message":{"key":"key","value":"value"}}`,
			want:    &ProduceMessagePayload{raw: true, message: Message{key: "key", value: "value"}},
		},
		{
			name:    "produce empty non-raw message",
			opcode:  ProduceMessage,
			payload: `{"raw":false,"message":{"key":"","value":""}}`,
			want:    &ProduceMessagePayload{},
		},
		{
			name:    "produce message at byte limit",
			opcode:  ProduceMessage,
			payload: fmt.Sprintf(`{"raw":false,"message":{"key":"","value":%q}}`, strings.Repeat("é", 512)),
			want:    &ProduceMessagePayload{message: Message{value: strings.Repeat("é", 512)}},
		},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, payload: `{}`, want: &ConsumerDisconnectPayload{}},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, payload: `{}`, want: &ConsumerHeartbeatPayload{}},
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
			want:     &ClientMetadata{clientId: "client-1", clientIPC: "/client-1"},
		},
		{name: "server", emitter: server, metadata: `{}`, want: &ServerMetadata{}},
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
				if gotOpcode := got.Opcode(); gotOpcode != p.opcode {
					t.Errorf("NewDataFromFrame(%q).Opcode() = %d, want %d", frame, gotOpcode, p.opcode)
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
