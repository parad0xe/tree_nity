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

// NewMessage creates a message with an internal offset of zero.
// NewData and Encode validate its size and strings.
func NewMessage(key, value string) Message {
	return Message{key: key, value: value}
}

// Key returns the message's key.
func (m Message) Key() string {
	return m.key
}

// Value returns the message's value.
func (m Message) Value() string {
	return m.value
}

// Offset returns the message's internal queue offset, which is not encoded.
func (m Message) Offset() int32 {
	return m.offset
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
