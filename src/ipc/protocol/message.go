package protocol

import (
	"fmt"
	"unicode/utf8"
)

// Message holds a message key, value, and queue offset.
type Message struct {
	offset int32
	key    string
	value  string
}

func (m *Message) toJSON() (messageJSON, error) {
	if err := m.validate(); err != nil {
		return messageJSON{}, err
	}
	return messageJSON{Key: &m.key, Value: &m.value}, nil
}

func (m *Message) validate() error {
	if m == nil {
		return ErrMissingMessage
	}
	if len(m.key)+len(m.value) > MessageSize {
		return ErrMessageTooLarge
	}
	if !utf8.ValidString(m.key) || !utf8.ValidString(m.value) {
		return fmt.Errorf("%w: message strings must contain valid UTF-8", ErrInvalidPayload)
	}
	return nil
}

type messageJSON struct {
	// Key is required and shares the MessageSize byte limit with Value.
	Key *string `json:"key"`
	// Value is required, but an empty string is accepted.
	Value *string `json:"value"`
}

func (m messageJSON) toMessage() (Message, error) {
	if err := m.validate(); err != nil {
		return Message{}, err
	}

	return Message{
		key:   *m.Key,
		value: *m.Value,
	}, nil
}

func (m messageJSON) validate() error {
	if m.Key == nil {
		return ErrMissingMessageKey
	}
	if m.Value == nil {
		return ErrMissingMessageValue
	}
	if len(*m.Key)+len(*m.Value) > MessageSize {
		return ErrMessageTooLarge
	}
	return nil
}
