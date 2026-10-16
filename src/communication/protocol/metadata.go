package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

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
		return nil, fmt.Errorf("unknown emitter: %d", emitter)
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(tmpMetadata); err != nil {
		return nil, err
	}

	return tmpMetadata, nil
}

type ClientMetadata struct {
	clientId  string
	clientIPC string
}

type clientMetadataJSON struct {
	ClientID  *string `json:"clientId"`
	ClientIPC *string `json:"clientIPC"`
}

func (m clientMetadataJSON) validate() error {
	if m.ClientID == nil {
		return errors.New("missing clientId")
	}
	if m.ClientIPC == nil {
		return errors.New("missing clientIPC")
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

type ServerMetadata struct {
}

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
