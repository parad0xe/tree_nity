package protocol

import (
	"errors"
)

type Metadata interface {
}

type metadataJSON interface {
	validate() error
	toMetadata() (Metadata, error)
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
	return ServerMetadata{}, nil
}
