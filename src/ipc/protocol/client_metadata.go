package protocol

import (
	"fmt"
	"unicode/utf8"
)

// ClientMetadata identifies a client and its IPC endpoint.
type ClientMetadata struct {
	clientId  string
	clientIPC string
}

// NewClientMetadata identifies a client and its return IPC endpoint.
// NewData and Encode validate its strings.
func NewClientMetadata(clientID, clientIPC string) *ClientMetadata {
	return &ClientMetadata{clientId: clientID, clientIPC: clientIPC}
}

// ClientID returns the client's identifier.
func (m ClientMetadata) ClientID() string {
	return m.clientId
}

// ClientIPC returns the client's return IPC endpoint.
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
	return &clientMetadataJSON{ClientID: &m.clientId, ClientIPC: &m.clientIPC}, nil
}

func (m *ClientMetadata) validate() error {
	if m == nil {
		return ErrMissingMetadata
	}
	if !utf8.ValidString(m.clientId) || !utf8.ValidString(m.clientIPC) {
		return fmt.Errorf("%w: client strings must contain valid UTF-8", ErrInvalidMetadata)
	}
	return nil
}
