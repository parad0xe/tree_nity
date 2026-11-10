package protocol

type clientMetadataJSON struct {
	// ClientIPC is required, although an empty string is accepted.
	ClientIPC *string `json:"clientIPC"`
}

func (m clientMetadataJSON) toMetadata() (Metadata, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	metadata := &ClientMetadata{
		clientIPC: *m.ClientIPC,
	}
	if err := metadata.validate(); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (m clientMetadataJSON) validate() error {
	if m.ClientIPC == nil {
		return ErrMissingClientIPC
	}
	return nil
}
