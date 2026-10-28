package protocol

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
