package consumer

import (
	"errors"
	"testing"
)

// TestNewConsumer checks metadata ownership and the supplied connection.
func TestNewConsumer(t *testing.T) {
	info := newInfo("consumer-1", 7, "events/", "/consumer-1")
	wantInfo := *info
	connection := &Connection{}
	c := newConsumer(info, connection)
	if c == nil {
		t.Errorf("newConsumer(%+v, %p) = nil, want a consumer", wantInfo, connection)
		return
	}
	if c.Info != wantInfo {
		t.Errorf("newConsumer(%+v, %p).Info = %+v, want %+v", wantInfo, connection, c.Info, wantInfo)
	}
	if c.Connection != connection {
		t.Errorf("newConsumer(%+v, %p).Connection = %p, want %p", wantInfo, connection, c.Connection, connection)
	}

	*info = *newInfo("other", 99, "other/", "/other")
	if c.Info != wantInfo {
		t.Errorf("consumer.Info after changing constructor input to %+v = %+v, want %+v", *info, c.Info, wantInfo)
	}
}

// TestConsumerValidate checks receiver, metadata, and connection validation.
func TestConsumerValidate(t *testing.T) {
	validInfo := *newInfo("consumer-1", 7, "events/", "/consumer-1")
	tests := []struct {
		name        string
		consumer    *Consumer
		wantErr     error
		wantMessage string
	}{
		{name: "nil receiver", wantErr: ErrUninitialized},
		{name: "zero value", consumer: &Consumer{}, wantMessage: "invalid consumer ID"},
		{
			name:        "invalid metadata",
			consumer:    &Consumer{Info: Info{id: "invalid/id"}, Connection: &Connection{}},
			wantMessage: "invalid consumer ID",
		},
		{name: "nil connection", consumer: &Consumer{Info: validInfo}, wantErr: ErrConnectionUninitialized},
		{name: "valid", consumer: &Consumer{Info: validInfo, Connection: &Connection{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.consumer.Validate()
			if tt.wantMessage != "" {
				if err == nil || err.Error() != tt.wantMessage {
					t.Errorf("Validate() on %+v error = %v, want %q", tt.consumer, err, tt.wantMessage)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() on %+v error = %v, want %v", tt.consumer, err, tt.wantErr)
			}
		})
	}
}
