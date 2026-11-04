package protocol

import (
	"encoding/base64"
	"fmt"
)

type messageJSON struct {
	// Key is required and base64-encoded.
	Key *string `json:"key"`
	// Value is required and base64-encoded; empty is valid.
	Value *string `json:"value"`
}

func (m messageJSON) toMessage() (Message, error) {
	if err := m.validate(); err != nil {
		return Message{}, err
	}

	key, err := base64.StdEncoding.DecodeString(*m.Key)
	if err != nil {
		return Message{}, fmt.Errorf("%w: message key: %v", ErrInvalidPayload, err)
	}
	value, err := base64.StdEncoding.DecodeString(*m.Value)
	if err != nil {
		return Message{}, fmt.Errorf("%w: message value: %v", ErrInvalidPayload, err)
	}
	message := NewMessage(key, value)
	if err := message.validate(); err != nil {
		return Message{}, err
	}
	return message, nil
}

func (m messageJSON) validate() error {
	if m.Key == nil {
		return ErrMissingMessageKey
	}
	if m.Value == nil {
		return ErrMissingMessageValue
	}
	return nil
}
