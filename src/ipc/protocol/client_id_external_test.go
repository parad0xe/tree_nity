package protocol_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
)

func consumerPayloadCases(clientID string) []struct {
	name    string
	opcode  protocol.Opcode
	payload protocol.Payload
	json    string
} {
	return []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
		json    string
	}{
		{name: "info", opcode: protocol.ConsumerInfo, payload: protocol.NewConsumerInfoPayload(clientID, "subscriber-1"), json: fmt.Sprintf(`{"clientId":%q,"subscriber":"subscriber-1"}`, clientID)},
		{name: "subscription", opcode: protocol.TopicSubscription, payload: protocol.NewTopicSubscriptionPayload(clientID, "orders", "", nil), json: fmt.Sprintf(`{"clientId":%q,"topic":"orders","prefix":""}`, clientID)},
		{name: "acknowledgement", opcode: protocol.Acknowledgement, payload: protocol.NewAcknowledgementPayload(clientID, 0), json: fmt.Sprintf(`{"clientId":%q,"offset":0}`, clientID)},
		{name: "heartbeat", opcode: protocol.ConsumerHeartbeat, payload: protocol.NewConsumerHeartbeatPayload(clientID), json: fmt.Sprintf(`{"clientId":%q}`, clientID)},
		{name: "disconnect", opcode: protocol.ConsumerDisconnect, payload: protocol.NewConsumerDisconnectPayload(clientID), json: fmt.Sprintf(`{"clientId":%q}`, clientID)},
	}
}

// TestConsumerClientIDRoundTrip checks that each consumer request carries its ID only in its payload.
func TestConsumerClientIDRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
	}{
		{name: "minimal", clientID: "c"},
		{name: "maximum length", clientID: strings.Repeat("c", 32)},
		{name: "mixed characters", clientID: "Client_09.test-1"},
	}
	for _, tt := range tests {
		for _, operation := range consumerPayloadCases(tt.clientID) {
			t.Run(operation.name+"/"+tt.name, func(t *testing.T) {
				data, err := protocol.NewData(operation.opcode, protocol.NewClientMetadata("/reply"), operation.payload)
				if err != nil {
					t.Errorf("NewData(%d, /reply, clientId=%q) error = %v, want nil", operation.opcode, tt.clientID, err)
					return
				}
				frame, err := protocol.Encode(data)
				want := fmt.Sprintf(`{"opcode":%d,"emitter":0,"metadata":{"clientIPC":"/reply"},"payload":%s}`, operation.opcode, operation.json)
				if err != nil || string(frame) != want {
					t.Errorf("Encode(opcode=%d, clientId=%q) = %q, error = %v, want %q and nil", operation.opcode, tt.clientID, frame, err, want)
					return
				}
				decoded, err := protocol.Decode(frame)
				if err != nil || !reflect.DeepEqual(decoded, data) {
					t.Errorf("Decode(%q) = %#v, error = %v, want %#v and nil", frame, decoded, err, data)
					return
				}
				payload, ok := decoded.Payload().(interface{ ClientID() string })
				if !ok {
					t.Errorf("Decode(%q).Payload() type = %T, want a ClientID accessor", frame, decoded.Payload())
					return
				}
				if got := payload.ClientID(); got != tt.clientID {
					t.Errorf("Decode(%q).ClientID() = %q, want %q", frame, got, tt.clientID)
				}
			})
		}
	}
}

// TestNewDataRejectsInvalidConsumerClientIDs checks ID validation on all public consumer constructors.
func TestNewDataRejectsInvalidConsumerClientIDs(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
	}{
		{name: "empty"},
		{name: "too long", clientID: strings.Repeat("c", 33)},
		{name: "large", clientID: strings.Repeat("c", 4096)},
		{name: "slash", clientID: "client/name"},
		{name: "non-ASCII", clientID: "cliént"},
		{name: "invalid UTF-8", clientID: "\xff"},
	}
	for _, tt := range tests {
		for _, operation := range consumerPayloadCases(tt.clientID) {
			t.Run(operation.name+"/"+tt.name, func(t *testing.T) {
				got, err := protocol.NewData(operation.opcode, protocol.NewClientMetadata("/reply"), operation.payload)
				if !errors.Is(err, protocol.ErrInvalidPayload) || !errors.Is(err, id.ErrInvalidID) || !reflect.DeepEqual(got, protocol.Data{}) {
					t.Errorf("NewData(%d, /reply, clientId=%q) = %#v, error = %v, want zero Data and errors matching %v and %v", operation.opcode, tt.clientID, got, err, protocol.ErrInvalidPayload, id.ErrInvalidID)
				}
			})
		}
	}
}

// TestDecodeRejectsInvalidConsumerClientIDs checks required IDs and their JSON types in all consumer requests.
func TestDecodeRejectsInvalidConsumerClientIDs(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr error
	}{
		{name: "missing", wantErr: protocol.ErrMissingClientID},
		{name: "null", value: `null`, wantErr: protocol.ErrMissingClientID},
		{name: "empty", value: `""`, wantErr: protocol.ErrInvalidPayload},
		{name: "too long", value: `"` + strings.Repeat("c", 33) + `"`, wantErr: protocol.ErrInvalidPayload},
		{name: "large", value: `"` + strings.Repeat("c", 4096) + `"`, wantErr: protocol.ErrInvalidPayload},
		{name: "slash", value: `"client/name"`, wantErr: protocol.ErrInvalidPayload},
		{name: "non-ASCII", value: `"cliént"`, wantErr: protocol.ErrInvalidPayload},
		{name: "number", value: `1`},
		{name: "boolean", value: `false`},
		{name: "array", value: `[]`},
		{name: "object", value: `{}`},
	}
	for _, operation := range consumerPayloadCases("client-1") {
		for _, tt := range tests {
			t.Run(operation.name+"/"+tt.name, func(t *testing.T) {
				previous := fmt.Sprintf(`{"opcode":%d,"emitter":0,"metadata":{"clientIPC":"/previous"},"payload":%s}`, operation.opcode, operation.json)
				if _, err := protocol.Decode([]byte(previous)); err != nil {
					t.Errorf("Decode(%q) error = %v, want nil", previous, err)
					return
				}
				var body map[string]json.RawMessage
				if err := json.Unmarshal([]byte(operation.json), &body); err != nil {
					t.Errorf("Unmarshal(%q) error = %v, want nil", operation.json, err)
					return
				}
				if tt.value == "" {
					delete(body, "clientId")
				} else {
					body["clientId"] = json.RawMessage(tt.value)
				}
				payload, err := json.Marshal(body)
				if err != nil {
					t.Errorf("Marshal(%#v) error = %v, want nil", body, err)
					return
				}
				frame := fmt.Sprintf(`{"opcode":%d,"emitter":0,"metadata":{"clientIPC":"/reply"},"payload":%s}`, operation.opcode, payload)
				got, err := protocol.Decode([]byte(frame))
				if err == nil || !reflect.DeepEqual(got, protocol.Data{}) {
					t.Errorf("Decode(%q) = %#v, error = %v, want zero Data and an error", frame, got, err)
				}
				if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
					t.Errorf("Decode(%q) error = %v, want %v", frame, err, tt.wantErr)
				}
				if tt.wantErr == nil {
					var typeErr *json.UnmarshalTypeError
					if !errors.As(err, &typeErr) || typeErr.Type != reflect.TypeFor[string]() {
						t.Errorf("Decode(%q) error = %v, want a JSON type error for string", frame, err)
					}
				}
			})
		}
	}
}

// TestNewDataRejectsZeroConsumerPayloads checks that consumer payload zero values lack a required ID.
func TestNewDataRejectsZeroConsumerPayloads(t *testing.T) {
	tests := []struct {
		name    string
		opcode  protocol.Opcode
		payload protocol.Payload
	}{
		{name: "info", opcode: protocol.ConsumerInfo, payload: &protocol.ConsumerInfoPayload{}},
		{name: "subscription", opcode: protocol.TopicSubscription, payload: &protocol.TopicSubscriptionPayload{}},
		{name: "acknowledgement", opcode: protocol.Acknowledgement, payload: &protocol.AcknowledgementPayload{}},
		{name: "heartbeat", opcode: protocol.ConsumerHeartbeat, payload: &protocol.ConsumerHeartbeatPayload{}},
		{name: "disconnect", opcode: protocol.ConsumerDisconnect, payload: &protocol.ConsumerDisconnectPayload{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := protocol.NewData(tt.opcode, protocol.NewClientMetadata("/reply"), tt.payload)
			if !errors.Is(err, protocol.ErrInvalidPayload) || !reflect.DeepEqual(got, protocol.Data{}) {
				t.Errorf("NewData(%d, /reply, %#v) = %#v, error = %v, want zero Data and %v", tt.opcode, tt.payload, got, err, protocol.ErrInvalidPayload)
			}
		})
	}
}
