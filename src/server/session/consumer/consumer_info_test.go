package consumer

import (
	"errors"
	"strings"
	"testing"

	id "tree_nity/src/server/utils"
)

// TestInfoAccessors checks that construction preserves every metadata field.
func TestInfoAccessors(t *testing.T) {
	tests := []struct {
		name string
		info Info
	}{
		{name: "empty optional fields", info: Info{id: "a"}},
		{name: "all fields", info: Info{id: "consumer-1", offset: 7, prefix: "events/", ipcPath: "/consumer-1"}},
		{
			name: "boundary values",
			info: Info{id: id.ID(strings.Repeat("a", 32)), offset: ^uint32(0), prefix: "é/猫/", ipcPath: "/consumer-max"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := newInfo(tt.info.id, tt.info.offset, tt.info.prefix, tt.info.ipcPath)
			if info == nil {
				t.Errorf("newInfo(%+v) = nil, want initialized metadata", tt.info)
				return
			}
			if got := info.ID(); got != tt.info.id {
				t.Errorf("ID() for input %+v = %q, want %q", tt.info, got, tt.info.id)
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

// TestInfoValidate checks missing metadata and ID validation boundaries.
func TestInfoValidate(t *testing.T) {
	tests := []struct {
		name        string
		info        *Info
		wantErr     error
		wantMessage string
	}{
		{name: "nil receiver", wantErr: ErrInfoUninitialized},
		{name: "zero value", info: &Info{}, wantMessage: "invalid consumer ID"},
		{name: "minimum ID", info: &Info{id: "a"}},
		{name: "allowed characters", info: &Info{id: "Ab0_.-z"}},
		{name: "maximum ID", info: &Info{id: id.ID(strings.Repeat("a", 32))}},
		{name: "ID too long", info: &Info{id: id.ID(strings.Repeat("a", 33))}, wantMessage: "invalid consumer ID"},
		{name: "invalid character", info: &Info{id: "consumer/name"}, wantMessage: "invalid consumer ID"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.info.validate()
			if tt.wantMessage != "" {
				if err == nil || err.Error() != tt.wantMessage {
					t.Errorf("validate() on %+v error = %v, want %q", tt.info, err, tt.wantMessage)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("validate() on %+v error = %v, want %v", tt.info, err, tt.wantErr)
			}
		})
	}
}
