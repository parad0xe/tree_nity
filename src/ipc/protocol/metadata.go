package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// Metadata holds emitter-specific data and converts it to JSON for its emitter.
type Metadata interface {
	toJSON(emitter Emitter) (metadataJSON, error)
	validate() error
}

type metadataJSON interface {
	validate() error
	toMetadata() (Metadata, error)
}

func newMetadataJSON(emitter Emitter, raw json.RawMessage) (metadataJSON, error) {
	var tmpMetadata metadataJSON

	switch emitter {
	case Client:
		tmpMetadata = &clientMetadataJSON{}
	case Server:
		tmpMetadata = &ServerMetadataJSON{}
	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownEmitter, emitter)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(tmpMetadata); err != nil {
		return nil, err
	}

	return tmpMetadata, nil
}

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

type clientMetadataJSON struct {
	// ClientID is required, although an empty string is accepted.
	ClientID *string `json:"clientId"`
	// ClientIPC is required, although an empty string is accepted.
	ClientIPC *string `json:"clientIPC"`
}

func (m clientMetadataJSON) toMetadata() (Metadata, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	return &ClientMetadata{
		clientId:  *m.ClientID,
		clientIPC: *m.ClientIPC,
	}, nil
}

func (m clientMetadataJSON) validate() error {
	if m.ClientID == nil {
		return ErrMissingClientID
	}
	if m.ClientIPC == nil {
		return ErrMissingClientIPC
	}
	return nil
}

// ServerMetadata represents server frames that carry no metadata fields.
type ServerMetadata struct {
}

// NewServerMetadata creates metadata for a server frame.
func NewServerMetadata() *ServerMetadata {
	return &ServerMetadata{}
}

func (s *ServerMetadata) toJSON(emitter Emitter) (metadataJSON, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if emitter != Server {
		return nil, fmt.Errorf("%w for emitter %d: %T", ErrInvalidMetadata, emitter, s)
	}
	return &ServerMetadataJSON{}, nil
}

func (s *ServerMetadata) validate() error {
	if s == nil {
		return ErrMissingMetadata
	}
	return nil
}

// ServerMetadataJSON is the empty JSON representation of server metadata.
type ServerMetadataJSON struct{}

func (s ServerMetadataJSON) toMetadata() (Metadata, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}

	return &ServerMetadata{}, nil
}

func (s ServerMetadataJSON) validate() error {
	return nil
}
