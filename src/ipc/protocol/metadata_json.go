package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type metadataJSON interface {
	validate() error
	toMetadata() (Metadata, error)
}

var metadataJSONFactories = map[Emitter]func() metadataJSON{
	Client: func() metadataJSON { return &clientMetadataJSON{} },
	Server: func() metadataJSON { return &serverMetadataJSON{} },
}

func newMetadataJSON(emitter Emitter, raw json.RawMessage) (metadataJSON, error) {
	createMetadata, ok := metadataJSONFactories[emitter]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrUnknownEmitter, emitter)
	}
	tmpMetadata := createMetadata()

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(tmpMetadata); err != nil {
		return nil, err
	}

	return tmpMetadata, nil
}
