package consumer

import (
	"errors"
	"strings"
	"testing"

	"tree_nity/src/ipc/id"
)

// TestInfoAccessors checks that construction preserves every metadata field.
func TestInfoAccessors(t *testing.T) {
	tests := []struct {
		name string
		info Info
	}{
		{name: "empty optional fields", info: Info{id: "a", topic: "t"}},
		{name: "all fields", info: Info{id: "consumer-1", topic: "events", offset: 7, prefix: "events/", ipcPath: "/consumer-1"}},
		{
			name: "boundary values",
			info: Info{id: id.ID(strings.Repeat("a", 32)), topic: strings.Repeat("t", 32), offset: ^uint32(0), prefix: "é/猫/", ipcPath: "/consumer-max"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := newInfo(tt.info.id, tt.info.topic, tt.info.offset, tt.info.prefix, tt.info.ipcPath)
			if info == nil {
				t.Errorf("newInfo(%+v) = nil, want initialized metadata", tt.info)
				return
			}
			if got := info.ID(); got != tt.info.id {
				t.Errorf("ID() for input %+v = %q, want %q", tt.info, got, tt.info.id)
			}
			if got := info.Topic(); got != tt.info.topic {
				t.Errorf("Topic() for input %+v = %q, want %q", tt.info, got, tt.info.topic)
			}
			if got := info.Offset(); got != tt.info.offset {
				t.Errorf("Offset() for input %+v = %d, want %d", tt.info, got, tt.info.offset)
			}
			if got := info.Prefix(); got != tt.info.prefix {
				t.Errorf("Prefix() for input %+v = %q, want %q", tt.info, got, tt.info.prefix)
			}
			if got := info.IPCPath(); got != tt.info.ipcPath {
				t.Errorf("IPCPath() for input %+v = %q, want %q", tt.info, got, tt.info.ipcPath)
			}
		})
	}
}

// TestInfoValidate checks valid ID boundaries.
func TestInfoValidate(t *testing.T) {
	tests := []struct {
		name string
		info *Info
	}{
		{name: "minimum ID", info: &Info{id: "a"}},
		{name: "allowed characters", info: &Info{id: "Ab0_.-z"}},
		{name: "maximum ID", info: &Info{id: id.ID(strings.Repeat("a", 32))}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.info.validate(); err != nil {
				t.Errorf("validate() on %+v error = %v, want nil", tt.info, err)
			}
		})
	}
}

// TestInfoValidateRejectsInvalidMetadata checks missing metadata and invalid IDs.
func TestInfoValidateRejectsInvalidMetadata(t *testing.T) {
	tests := []struct {
		name    string
		info    *Info
		wantErr error
	}{
		{name: "nil receiver", wantErr: ErrInfoUninitialized},
		{name: "zero value", info: &Info{}, wantErr: id.ErrInvalidID},
		{name: "ID too long", info: &Info{id: id.ID(strings.Repeat("a", 33))}, wantErr: id.ErrInvalidID},
		{name: "invalid character", info: &Info{id: "consumer/name"}, wantErr: id.ErrInvalidID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.info.validate(); !errors.Is(err, tt.wantErr) {
				t.Errorf("validate() on %+v error = %v, want %v", tt.info, err, tt.wantErr)
			}
		})
	}
}
