package protocol

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"tree_nity/src/ipc/id"
)

type failingMetadata struct {
	err error
}

func (m *failingMetadata) toJSON(_ Emitter) (metadataJSON, error) {
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

func (p *failingPayload) toJSON(_ Emitter, _ Opcode) (payloadJSON, error) {
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

// TestEncode checks the wire representation and round trips for every client opcode.
func TestEncode(t *testing.T) {
	payloads := []struct {
		name    string
		opcode  Opcode
		payload Payload
		json    string
	}{
		{name: "list topic", opcode: ListTopic, payload: &ListTopicPayload{}, json: `{}`},
		{name: "create topic", opcode: CreateTopic, payload: &CreateTopicPayload{topic: "orders"}, json: `{"topic":"orders"}`},
		{name: "consumer info", opcode: ConsumerInfo, payload: &ConsumerInfoPayload{}, json: `{}`},
		{name: "topic subscription", opcode: TopicSubscription, payload: &TopicSubscriptionPayload{topic: "orders"}, json: `{"topic":"orders","prefix":""}`},
		{name: "acknowledgement", opcode: Acknowledgement, payload: &AcknowledgementPayload{}, json: `{}`},
		{name: "check topic", opcode: CheckTopic, payload: &CheckTopicPayload{topic: "orders"}, json: `{"topic":"orders"}`},
		{
			name:    "produce message",
			opcode:  ProduceMessage,
			payload: &ProduceMessagePayload{topic: "orders", message: Message{key: []byte("key"), value: []byte("value")}},
			json:    `{"topic":"orders","message":{"key":"a2V5","value":"dmFsdWU="}}`,
		},
		{
			name:    "produce empty message",
			opcode:  ProduceMessage,
			payload: &ProduceMessagePayload{topic: "orders"},
			json:    `{"topic":"orders","message":{"key":"","value":""}}`,
		},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, payload: &ConsumerDisconnectPayload{}, json: `{}`},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, payload: &ConsumerHeartbeatPayload{}, json: `{}`},
	}
	emitters := []struct {
		name     string
		emitter  Emitter
		metadata Metadata
		json     string
	}{
		{
			name:     "client",
			emitter:  Client,
			metadata: &ClientMetadata{clientID: "client-1", clientIPC: "/client-1"},
			json:     `{"clientId":"client-1","clientIPC":"/client-1"}`,
		},
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

// TestCodecAcceptsValidClientIDs checks client ID boundaries and allowed characters in both directions.
func TestCodecAcceptsValidClientIDs(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
	}{
		{name: "one character", clientID: "c"},
		{name: "32 characters", clientID: strings.Repeat("c", 32)},
		{name: "lowercase letters", clientID: "abcdefghijklmnopqrstuvwxyz"},
		{name: "uppercase letters", clientID: "ABCDEFGHIJKLMNOPQRSTUVWXYZ"},
		{name: "digits", clientID: "0123456789"},
		{name: "punctuation", clientID: "_.-"},
		{name: "mixed characters", clientID: "Client_09.test-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := Data{opcode: ListTopic, emitter: Client, metadata: &ClientMetadata{clientID: tt.clientID}, payload: &ListTopicPayload{}}
			wantFrame := fmt.Sprintf(`{"opcode":0,"emitter":0,"metadata":{"clientId":%q,"clientIPC":""},"payload":{}}`, tt.clientID)
			frame, err := Encode(data)
			if err != nil || string(frame) != wantFrame {
				t.Errorf("Encode(clientID=%q) = %q, error = %v, want %q and nil", tt.clientID, frame, err, wantFrame)
			}

			got, err := Decode([]byte(wantFrame))
			if err != nil || !reflect.DeepEqual(got, data) {
				t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", wantFrame, got, err, data)
			}
		})
	}
}

// TestCodecRejectsInvalidClientIDs checks format errors and their identity in both directions.
func TestCodecRejectsInvalidClientIDs(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
	}{
		{name: "empty"},
		{name: "33 characters", clientID: strings.Repeat("c", 33)},
		{name: "4096 characters", clientID: strings.Repeat("c", 4096)},
		{name: "space", clientID: "client name"},
		{name: "slash", clientID: "client/name"},
		{name: "non-ASCII letters", clientID: "cliént"},
		{name: "non-ASCII symbol", clientID: "猫"},
		{name: "quote", clientID: "client\"name"},
		{name: "backslash", clientID: "client\\name"},
		{name: "newline", clientID: "client\nname"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := Data{opcode: ListTopic, emitter: Client, metadata: &ClientMetadata{clientID: tt.clientID}, payload: &ListTopicPayload{}}
			frame, err := Encode(data)
			if !errors.Is(err, ErrInvalidMetadata) || !errors.Is(err, id.ErrInvalidID) || frame != nil {
				t.Errorf("Encode(clientID=%q) = %q, error = %v, want nil frame and errors matching %v and %v", tt.clientID, frame, err, ErrInvalidMetadata, id.ErrInvalidID)
			}

			input := fmt.Sprintf(`{"opcode":0,"emitter":0,"metadata":{"clientId":%q,"clientIPC":""},"payload":{}}`, tt.clientID)
			got, err := Decode([]byte(input))
			if !errors.Is(err, ErrInvalidMetadata) || !errors.Is(err, id.ErrInvalidID) || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and errors matching %v and %v", input, got, err, ErrInvalidMetadata, id.ErrInvalidID)
			}
		})
	}
}

// TestEncodeStringsAndLimits checks escaping, byte boundaries, and the internal offset.
func TestEncodeStringsAndLimits(t *testing.T) {
	tests := []struct {
		name     string
		metadata ClientMetadata
		message  Message
	}{
		{name: "empty IPC and message strings", metadata: ClientMetadata{clientID: "client"}},
		{name: "minimal strings", metadata: ClientMetadata{clientID: "c", clientIPC: "q"}, message: Message{key: []byte("k"), value: []byte("v")}},
		{name: "non-UTF-8 key", metadata: ClientMetadata{clientID: "client"}, message: Message{key: []byte("\xff")}},
		{name: "non-UTF-8 value", metadata: ClientMetadata{clientID: "client"}, message: Message{value: []byte("\xff")}},
		{
			name:     "escaped strings",
			metadata: ClientMetadata{clientID: "client", clientIPC: "/client\"\\\n\x00/é/猫<&>\u2028"},
			message:  Message{key: []byte("key\"\\\n\t\x00"), value: []byte("é/猫<&>\u2029")},
		},
		{name: "key at byte limit", metadata: ClientMetadata{clientID: "client"}, message: Message{key: []byte(strings.Repeat("k", MessageSize))}},
		{name: "value at byte limit", metadata: ClientMetadata{clientID: "client"}, message: Message{value: []byte(strings.Repeat("v", MessageSize))}},
		{name: "combined byte limit", metadata: ClientMetadata{clientID: "client"}, message: Message{key: []byte(strings.Repeat("k", MessageSize/2)), value: []byte(strings.Repeat("v", MessageSize/2))}},
		{name: "UTF-8 at byte limit", metadata: ClientMetadata{clientID: "client"}, message: Message{value: []byte(strings.Repeat("é", MessageSize/2))}},
		{name: "null bytes at byte limit", metadata: ClientMetadata{clientID: "client"}, message: Message{value: []byte(strings.Repeat("\x00", MessageSize))}},
		{
			name:     "maximum client ID and large IPC",
			metadata: ClientMetadata{clientID: strings.Repeat("c", 32), clientIPC: strings.Repeat("q", 4096)},
		},
		{name: "internal offset", metadata: ClientMetadata{clientID: "client"}, message: Message{offset: 42, key: []byte("key"), value: []byte("value")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := Data{opcode: ProduceMessage, emitter: Client, metadata: &tt.metadata, payload: &ProduceMessagePayload{topic: "orders", message: tt.message}}
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
				emitter:  Client,
				metadata: &tt.metadata,
				payload:  &ProduceMessagePayload{topic: "orders", message: Message{key: tt.message.key, value: tt.message.value}},
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
		{name: "negative opcode", data: Data{opcode: -1, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &ListTopicPayload{}}, wantErr: ErrUnknownOpcode},
		{name: "unknown opcode", data: Data{opcode: 9, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &ListTopicPayload{}}, wantErr: ErrUnknownOpcode},
		{name: "missing client metadata", data: Data{emitter: Client, payload: &ListTopicPayload{}}, wantErr: ErrMissingMetadata},
		{name: "missing server metadata", data: Data{emitter: Server, payload: &ListTopicPayload{}}, wantErr: ErrMissingMetadata},
		{name: "nil client metadata pointer", data: Data{emitter: Client, metadata: (*ClientMetadata)(nil), payload: &ListTopicPayload{}}, wantErr: ErrMissingMetadata},
		{name: "nil server metadata pointer", data: Data{emitter: Server, metadata: (*ServerMetadata)(nil), payload: &ListTopicPayload{}}, wantErr: ErrMissingMetadata},
		{name: "server metadata from client", data: Data{emitter: Client, metadata: &ServerMetadata{}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "client metadata from server", data: Data{emitter: Server, metadata: &ClientMetadata{clientID: "client"}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "metadata conversion error", data: Data{emitter: Server, metadata: &failingMetadata{err: ErrInvalidMetadata}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "wrapped metadata conversion error", data: Data{emitter: Server, metadata: &failingMetadata{err: fmt.Errorf("metadata: %w", conversionErr)}, payload: &ListTopicPayload{}}, wantErr: conversionErr},
		{name: "invalid UTF-8 client ID", data: Data{emitter: Client, metadata: &ClientMetadata{clientID: "\xff"}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "invalid UTF-8 client IPC", data: Data{emitter: Client, metadata: &ClientMetadata{clientID: "client", clientIPC: "\xff"}, payload: &ListTopicPayload{}}, wantErr: ErrInvalidMetadata},
		{name: "missing payload", data: Data{emitter: Client, metadata: &ClientMetadata{clientID: "client"}}, wantErr: ErrMissingPayload},
		{name: "payload conversion error", data: Data{emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &failingPayload{err: ErrInvalidPayload}}, wantErr: ErrInvalidPayload},
		{name: "wrapped payload conversion error", data: Data{emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &failingPayload{err: fmt.Errorf("payload: %w", conversionErr)}}, wantErr: conversionErr},
		{name: "empty topic", data: Data{opcode: CreateTopic, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &CreateTopicPayload{}}, wantErr: ErrInvalidPayload},
		{name: "invalid topic", data: Data{opcode: CreateTopic, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &CreateTopicPayload{topic: "user/events"}}, wantErr: ErrInvalidPayload},
		{name: "empty check topic", data: Data{opcode: CheckTopic, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &CheckTopicPayload{}}, wantErr: ErrInvalidPayload},
		{name: "invalid check topic", data: Data{opcode: CheckTopic, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &CheckTopicPayload{topic: "user/events"}}, wantErr: ErrInvalidPayload},
		{
			name:    "oversized key",
			data:    Data{opcode: ProduceMessage, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &ProduceMessagePayload{topic: "orders", message: Message{key: []byte(strings.Repeat("k", MessageSize+1))}}},
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "oversized value",
			data:    Data{opcode: ProduceMessage, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &ProduceMessagePayload{topic: "orders", message: Message{value: []byte(strings.Repeat("v", MessageSize+1))}}},
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "combined size exceeds limit",
			data:    Data{opcode: ProduceMessage, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &ProduceMessagePayload{topic: "orders", message: Message{key: []byte(strings.Repeat("k", MessageSize/2)), value: []byte(strings.Repeat("v", MessageSize/2+1))}}},
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "UTF-8 exceeds byte limit",
			data:    Data{opcode: ProduceMessage, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &ProduceMessagePayload{topic: "orders", message: Message{key: []byte("k"), value: []byte(strings.Repeat("é", MessageSize/2))}}},
			wantErr: ErrMessageTooLarge,
		},
		{name: "empty produce topic", data: Data{opcode: ProduceMessage, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &ProduceMessagePayload{}}, wantErr: ErrInvalidPayload},
		{name: "invalid produce topic", data: Data{opcode: ProduceMessage, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: &ProduceMessagePayload{topic: "user/events"}}, wantErr: ErrInvalidPayload},
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
		{name: "create topic", opcode: CreateTopic, payload: &CreateTopicPayload{topic: "orders"}, nilPtr: (*CreateTopicPayload)(nil)},
		{name: "consumer info", opcode: ConsumerInfo, payload: &ConsumerInfoPayload{}, nilPtr: (*ConsumerInfoPayload)(nil)},
		{name: "topic subscription", opcode: TopicSubscription, payload: &TopicSubscriptionPayload{topic: "orders"}, nilPtr: (*TopicSubscriptionPayload)(nil)},
		{name: "acknowledgement", opcode: Acknowledgement, payload: &AcknowledgementPayload{}, nilPtr: (*AcknowledgementPayload)(nil)},
		{name: "check topic", opcode: CheckTopic, payload: &CheckTopicPayload{topic: "orders"}, nilPtr: (*CheckTopicPayload)(nil)},
		{name: "produce message", opcode: ProduceMessage, payload: &ProduceMessagePayload{topic: "orders"}, nilPtr: (*ProduceMessagePayload)(nil)},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, payload: &ConsumerDisconnectPayload{}, nilPtr: (*ConsumerDisconnectPayload)(nil)},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, payload: &ConsumerHeartbeatPayload{}, nilPtr: (*ConsumerHeartbeatPayload)(nil)},
	}
	for _, operation := range payloads {
		t.Run(operation.name+"/nil pointer", func(t *testing.T) {
			data := Data{opcode: operation.opcode, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: operation.nilPtr}
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
				data := Data{opcode: operation.opcode, emitter: Client, metadata: &ClientMetadata{clientID: "client"}, payload: p.payload}
				frame, err := Encode(data)
				if !errors.Is(err, ErrInvalidPayload) || frame != nil {
					t.Errorf("Encode(%#v) = %q, error = %v, want nil frame and %v", data, frame, err, ErrInvalidPayload)
				}
			})
		}
	}
}

// TestDecode checks every opcode for client frames.
func TestDecode(t *testing.T) {
	payloads := []struct {
		name    string
		opcode  Opcode
		payload string
		want    Payload
	}{
		{name: "list topic", opcode: ListTopic, payload: `{}`, want: &ListTopicPayload{}},
		{name: "create topic", opcode: CreateTopic, payload: `{"topic":"orders"}`, want: &CreateTopicPayload{topic: "orders"}},
		{name: "consumer info", opcode: ConsumerInfo, payload: `{}`, want: &ConsumerInfoPayload{}},
		{name: "topic subscription", opcode: TopicSubscription, payload: `{"topic":"orders","prefix":""}`, want: &TopicSubscriptionPayload{topic: "orders"}},
		{name: "acknowledgement", opcode: Acknowledgement, payload: `{}`, want: &AcknowledgementPayload{}},
		{name: "check topic", opcode: CheckTopic, payload: `{"topic":"orders"}`, want: &CheckTopicPayload{topic: "orders"}},
		{
			name:    "produce message",
			opcode:  ProduceMessage,
			payload: `{"topic":"orders","message":{"key":"a2V5","value":"dmFsdWU="}}`,
			want:    &ProduceMessagePayload{topic: "orders", message: Message{key: []byte("key"), value: []byte("value")}},
		},
		{
			name:    "produce empty message",
			opcode:  ProduceMessage,
			payload: `{"topic":"orders","message":{"key":"","value":""}}`,
			want:    &ProduceMessagePayload{topic: "orders"},
		},
		{
			name:    "produce message at byte limit",
			opcode:  ProduceMessage,
			payload: fmt.Sprintf(`{"topic":"orders","message":{"key":"","value":%q}}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("é", 512)))),
			want:    &ProduceMessagePayload{topic: "orders", message: Message{value: []byte(strings.Repeat("é", 512))}},
		},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, payload: `{}`, want: &ConsumerDisconnectPayload{}},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, payload: `{}`, want: &ConsumerHeartbeatPayload{}},
	}
	emitters := []struct {
		name     string
		emitter  Emitter
		metadata string
		want     Metadata
	}{
		{
			name:     "client",
			emitter:  Client,
			metadata: `{"clientId":"client-1","clientIPC":"/client-1"}`,
			want:     &ClientMetadata{clientID: "client-1", clientIPC: "/client-1"},
		},
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
					t.Errorf("Decode(%q).Opcode() = %d, want %d", frame, gotOpcode, p.opcode)
				}
			})
		}
	}
}

// TestEncodeRejectsClientPayloadsFromServer checks emitter-specific payload validation.
func TestEncodeRejectsClientPayloadsFromServer(t *testing.T) {
	tests := []struct {
		name    string
		opcode  Opcode
		payload Payload
		wantErr error
	}{
		{name: "list topic", opcode: ListTopic, payload: &ListTopicPayload{}, wantErr: ErrInvalidPayload},
		{name: "create topic", opcode: CreateTopic, payload: &CreateTopicPayload{topic: "orders"}, wantErr: ErrInvalidPayload},
		{name: "consumer info", opcode: ConsumerInfo, payload: &ConsumerInfoPayload{}, wantErr: ErrInvalidPayload},
		{name: "topic subscription", opcode: TopicSubscription, payload: &TopicSubscriptionPayload{topic: "orders"}, wantErr: ErrInvalidPayload},
		{name: "acknowledgement", opcode: Acknowledgement, payload: &AcknowledgementPayload{}, wantErr: ErrInvalidPayload},
		{name: "check topic", opcode: CheckTopic, payload: &CheckTopicPayload{topic: "orders"}, wantErr: ErrInvalidPayload},
		{
			name: "produce message", opcode: ProduceMessage,
			payload: &ProduceMessagePayload{topic: "orders", message: Message{key: []byte("key"), value: []byte("value")}},
			wantErr: ErrInvalidPayload,
		},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, payload: &ConsumerDisconnectPayload{}, wantErr: ErrInvalidPayload},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, payload: &ConsumerHeartbeatPayload{}, wantErr: ErrUnknownOpcode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := Data{opcode: tt.opcode, emitter: Server, metadata: &ServerMetadata{}, payload: tt.payload}
			frame, err := Encode(data)
			if !errors.Is(err, tt.wantErr) || frame != nil {
				t.Errorf("Encode(%#v) = %q, error = %v, want nil and %v", data, frame, err, tt.wantErr)
			}
		})
	}
}

// TestDecodeRejectsInvalidServerPayloads checks client JSON that cannot form a server payload.
func TestDecodeRejectsInvalidServerPayloads(t *testing.T) {
	tests := []struct {
		name    string
		opcode  Opcode
		json    string
		wantErr error
	}{
		{name: "list topic", opcode: ListTopic, json: `{}`, wantErr: ErrMissingTopics},
		{name: "create topic", opcode: CreateTopic, json: `{"topic":"orders"}`},
		{name: "consumer info", opcode: ConsumerInfo, json: `{}`, wantErr: ErrUnknownOpcode},
		{name: "topic subscription", opcode: TopicSubscription, json: `{"topic":"orders","prefix":""}`},
		{name: "check topic", opcode: CheckTopic, json: `{"topic":"orders"}`},
		{name: "produce message", opcode: ProduceMessage, json: `{"topic":"orders","message":{"key":"a2V5","value":"dmFsdWU="}}`, wantErr: ErrUnknownOpcode},
		{name: "consumer disconnect", opcode: ConsumerDisconnect, json: `{}`, wantErr: ErrUnknownOpcode},
		{name: "consumer heartbeat", opcode: ConsumerHeartbeat, json: `{}`, wantErr: ErrUnknownOpcode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := fmt.Sprintf(`{"opcode":%d,"emitter":1,"metadata":{},"payload":%s}`, tt.opcode, tt.json)
			got, err := Decode([]byte(input))
			if err == nil || !reflect.DeepEqual(got, Data{}) {
				t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and an error", input, got, err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("Decode(%q) error = %v, want %v", input, err, tt.wantErr)
			}
		})
	}
}

// TestDecodeSelectsPayloadByEmitter checks the shared acknowledgement and topic reply opcode.
func TestDecodeSelectsPayloadByEmitter(t *testing.T) {
	tests := []struct {
		name     string
		emitter  Emitter
		metadata string
		want     Data
	}{
		{
			name: "client acknowledgement", emitter: Client,
			metadata: `{"clientId":"client-1","clientIPC":"/client-1"}`,
			want:     Data{opcode: Acknowledgement, emitter: Client, metadata: &ClientMetadata{clientID: "client-1", clientIPC: "/client-1"}, payload: &AcknowledgementPayload{}},
		},
		{
			name: "server topic check reply", emitter: Server,
			metadata: `{}`,
			want:     Data{opcode: CheckTopicReply, emitter: Server, metadata: &ServerMetadata{}, payload: &CheckTopicReplyPayload{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := fmt.Sprintf(`{"opcode":4,"emitter":%d,"metadata":%s,"payload":{}}`, tt.emitter, tt.metadata)
			got, err := Decode([]byte(frame))
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, got, err, tt.want)
			}
		})
	}
}

// TestDecodeRejectsInvalidInput checks malformed and incomplete frames.
func TestDecodeRejectsInvalidInput(t *testing.T) {
	validFrame := `{"opcode":0,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{}}`
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
		{name: "unknown frame field", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{},"extra":true}`},
		{name: "missing opcode", input: `{"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{}}`, wantErr: ErrMissingOpcode},
		{name: "null opcode", input: `{"opcode":null,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{}}`, wantErr: ErrMissingOpcode},
		{name: "negative opcode", input: `{"opcode":-1,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{}}`, wantErr: ErrUnknownOpcode},
		{name: "unknown opcode", input: `{"opcode":9,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{}}`, wantErr: ErrUnknownOpcode},
		{name: "string opcode", input: `{"opcode":"0","emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{}}`},
		{name: "fractional opcode", input: `{"opcode":0.5,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{}}`},
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
		{name: "null clientId", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":null,"clientIPC":"/client"},"payload":{}}`, wantErr: ErrMissingClientID},
		{name: "missing clientIPC", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":"client"},"payload":{}}`, wantErr: ErrMissingClientIPC},
		{name: "null clientIPC", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":"client","clientIPC":null},"payload":{}}`, wantErr: ErrMissingClientIPC},
		{name: "missing payload", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"}}`, wantErr: ErrMissingPayload},
		{name: "null payload", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":null}`, wantErr: ErrMissingPayload},
		{name: "array payload", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":[]}`},
		{name: "unknown payload field", input: `{"opcode":0,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"extra":true}}`},
		{name: "missing topic", input: `{"opcode":1,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{}}`, wantErr: ErrMissingTopic},
		{name: "missing check topic", input: `{"opcode":5,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{}}`, wantErr: ErrMissingTopic},
		{name: "missing produce topic", input: `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"message":{"key":"aw==","value":"dg=="}}}`, wantErr: ErrMissingTopic},
		{name: "missing message", input: `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders"}}`, wantErr: ErrMissingMessage},
		{name: "missing message key", input: `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"value":"dg=="}}}`, wantErr: ErrMissingMessageKey},
		{name: "missing message value", input: `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"key":"aw=="}}}`, wantErr: ErrMissingMessageValue},
		{name: "unknown message field", input: `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"key":"aw==","value":"dg==","extra":true}}}`},
		{name: "array message key", input: `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"key":[0,255],"value":""}}}`},
		{name: "array message value", input: `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"key":"","value":[0,255]}}}`},
		{name: "invalid base64 key", input: `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"key":"?","value":""}}}`, wantErr: ErrInvalidPayload},
		{name: "invalid base64 value", input: `{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"key":"","value":"?"}}}`, wantErr: ErrInvalidPayload},
		{
			name:    "oversized message",
			input:   fmt.Sprintf(`{"opcode":6,"emitter":0,"metadata":{"clientId":"client-1","clientIPC":"/client-1"},"payload":{"topic":"orders","message":{"key":"aw==","value":%q}}}`, base64.StdEncoding.EncodeToString([]byte(strings.Repeat("v", 1024)))),
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
