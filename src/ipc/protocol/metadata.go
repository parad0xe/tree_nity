package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Metadata holds frame metadata decoded according to its emitter.
type Metadata interface {
}

type metadataJSON interface {
	validate() error
	toMetadata() (Metadata, error)
}

func newMetadataJSON(emitter emitter, raw json.RawMessage) (metadataJSON, error) {
	var tmpMetadata metadataJSON

	switch emitter {
	case client:
		tmpMetadata = &clientMetadataJSON{}
	case server:
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

type clientMetadataJSON struct {
	// ClientID is required, although an empty string is accepted.
	ClientID *string `json:"clientId"`
	// ClientIPC is required, although an empty string is accepted.
	ClientIPC *string `json:"clientIPC"`
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

func (m clientMetadataJSON) toMetadata() (Metadata, error) {
	if err := m.validate(); err != nil {
		return ClientMetadata{}, err
	}

	return ClientMetadata{
		clientId:  *m.ClientID,
		clientIPC: *m.ClientIPC,
	}, nil
}

// ServerMetadata represents server frames that carry no metadata fields.
type ServerMetadata struct {
}

// ServerMetadataJSON is the empty JSON representation of server metadata.
type ServerMetadataJSON struct{}

func (s ServerMetadataJSON) validate() error {
	return nil
}

func (s ServerMetadataJSON) toMetadata() (Metadata, error) {
	if err := s.validate(); err != nil {
		return ServerMetadata{}, err
	}

	return ServerMetadata{}, nil
}
