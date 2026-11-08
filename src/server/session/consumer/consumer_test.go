package consumer

import (
	"errors"
	"testing"

	"tree_nity/src/ipc/id"
)

// TestNewConsumer checks metadata ownership and disconnected state.
func TestNewConsumer(t *testing.T) {
	info := newInfo("consumer-1", "events", 7, "events/", "/consumer-1")
	wantInfo := *info
	c := newConsumer(info)
	if err := c.Validate(); err != nil {
		t.Errorf("newConsumer(%+v) validation error = %v, want nil", wantInfo, err)
		return
	}
	if c.Info != wantInfo {
		t.Errorf("newConsumer(%+v).Info = %+v, want %+v", wantInfo, c.Info, wantInfo)
	}
	if connection := c.Connection(); connection != nil {
		t.Errorf("Connection() after newConsumer(%+v) = %p, want nil", wantInfo, connection)
	}

	*info = *newInfo("other", "other-topic", 99, "other/", "/other")
	if c.Info != wantInfo {
		t.Errorf("consumer.Info after changing constructor input to %+v = %+v, want %+v", *info, c.Info, wantInfo)
	}
}

// TestConsumerConnectionRejectsInvalidConsumer checks invalid receivers.
func TestConsumerConnectionRejectsInvalidConsumer(t *testing.T) {
	tests := []struct {
		name     string
		consumer *Consumer
	}{
		{name: "nil receiver"},
		{name: "zero value", consumer: &Consumer{}},
		{name: "invalid metadata", consumer: &Consumer{Info: Info{id: "invalid/id"}, connection: &Connection{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.consumer.Connection(); got != nil {
				t.Errorf("Connection() for %s = %p, want nil", tt.name, got)
			}
		})
	}
}

// TestConsumerValidate checks a consumer with valid metadata and a connection.
func TestConsumerValidate(t *testing.T) {
	c := &Consumer{
		Info:       *newInfo("consumer-1", "events", 7, "events/", "/consumer-1"),
		connection: &Connection{},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate() on %+v error = %v, want nil", c, err)
	}
}

// TestConsumerValidateDisconnected checks metadata without a connection.
func TestConsumerValidateDisconnected(t *testing.T) {
	c := newConsumer(newInfo("consumer-1", "events", 7, "events/", "/missing"))
	if err := c.Validate(); err != nil {
		t.Errorf("Validate() for disconnected consumer %q error = %v, want nil", c.Info.id, err)
	}
}

// TestConsumerValidateRejectsInvalidState checks receiver and metadata errors.
func TestConsumerValidateRejectsInvalidState(t *testing.T) {
	tests := []struct {
		name     string
		consumer *Consumer
		wantErr  error
	}{
		{name: "nil receiver", wantErr: ErrUninitialized},
		{name: "zero value", consumer: &Consumer{}, wantErr: id.ErrInvalidID},
		{
			name:     "invalid metadata",
			consumer: &Consumer{Info: Info{id: "invalid/id"}, connection: &Connection{}},
			wantErr:  id.ErrInvalidID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.consumer.Validate(); !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() on %+v error = %v, want %v", tt.consumer, err, tt.wantErr)
			}
		})
	}
}
