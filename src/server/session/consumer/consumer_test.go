package consumer

import (
	"errors"
	"testing"

	"tree_nity/src/ipc/id"
)

// TestNewConsumer checks metadata ownership and the supplied connection.
func TestNewConsumer(t *testing.T) {
	info := newInfo("consumer-1", "events", 7, "events/", "/consumer-1")
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

	*info = *newInfo("other", "other-topic", 99, "other/", "/other")
	if c.Info != wantInfo {
		t.Errorf("consumer.Info after changing constructor input to %+v = %+v, want %+v", *info, c.Info, wantInfo)
	}
}

// TestConsumerValidate checks a consumer with valid metadata and a connection.
func TestConsumerValidate(t *testing.T) {
	c := &Consumer{
		Info:       *newInfo("consumer-1", "events", 7, "events/", "/consumer-1"),
		Connection: &Connection{},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate() on %+v error = %v, want nil", c, err)
	}
}

// TestConsumerValidateRejectsInvalidState checks receiver, metadata, and connection errors.
func TestConsumerValidateRejectsInvalidState(t *testing.T) {
	validInfo := *newInfo("consumer-1", "events", 7, "events/", "/consumer-1")
	tests := []struct {
		name     string
		consumer *Consumer
		wantErr  error
	}{
		{name: "nil receiver", wantErr: ErrUninitialized},
		{name: "zero value", consumer: &Consumer{}, wantErr: id.ErrInvalidID},
		{
			name:     "invalid metadata",
			consumer: &Consumer{Info: Info{id: "invalid/id"}, Connection: &Connection{}},
			wantErr:  id.ErrInvalidID,
		},
		{name: "nil connection", consumer: &Consumer{Info: validInfo}, wantErr: ErrConnectionUninitialized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.consumer.Validate(); !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() on %+v error = %v, want %v", tt.consumer, err, tt.wantErr)
			}
		})
	}
}
