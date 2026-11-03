package protocol

type clientMetadataJSON struct {
	// ClientID is required and validated by ClientMetadata.
	ClientID *string `json:"clientId"`
	// ClientIPC is required, although an empty string is accepted.
	ClientIPC *string `json:"clientIPC"`
}

func (m clientMetadataJSON) toMetadata() (Metadata, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	metadata := &ClientMetadata{
		clientID:  *m.ClientID,
		clientIPC: *m.ClientIPC,
	}
	if err := metadata.validate(); err != nil {
		return nil, err
	}
	return metadata, nil
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
