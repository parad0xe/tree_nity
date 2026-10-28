package protocol

type serverMetadataJSON struct{}

func (s serverMetadataJSON) toMetadata() (Metadata, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}

	return &ServerMetadata{}, nil
}

func (s serverMetadataJSON) validate() error {
	return nil
}
