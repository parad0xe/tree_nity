package protocol

import "fmt"

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
	return &serverMetadataJSON{}, nil
}

func (s *ServerMetadata) validate() error {
	if s == nil {
		return ErrMissingMetadata
	}
	return nil
}
