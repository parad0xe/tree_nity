package protocol

import (
	"fmt"
	"unicode/utf8"
)

// ClientMetadata identifies a client's IPC endpoint.
type ClientMetadata struct {
	clientIPC string
}

// NewClientMetadata returns metadata identifying a client's IPC endpoint.
func NewClientMetadata(clientIPC string) *ClientMetadata {
	return &ClientMetadata{clientIPC: clientIPC}
}

// ClientIPC returns the client's IPC endpoint.
func (m ClientMetadata) ClientIPC() string {
	return m.clientIPC
}

func (m *ClientMetadata) toJSON(emitter Emitter) (metadataJSON, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	if emitter != Client {
		return nil, fmt.Errorf("%w for emitter %d: %T", ErrInvalidMetadata, emitter, m)
	}
	return &clientMetadataJSON{ClientIPC: &m.clientIPC}, nil
}

func (m *ClientMetadata) validate() error {
	if m == nil {
		return ErrMissingMetadata
	}
	if !utf8.ValidString(m.clientIPC) {
		return fmt.Errorf("%w: clientIPC must contain valid UTF-8", ErrInvalidMetadata)
	}
	return nil
}
