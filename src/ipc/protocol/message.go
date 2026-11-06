package protocol

import "encoding/base64"

// Message holds a binary key, value, and uint32 queue offset.
type Message struct {
	offset uint32
	key    []byte
	value  []byte
}

// NewMessage copies key and value with a zero offset.
func NewMessage(key, value []byte) Message {
	return Message{key: append([]byte(nil), key...), value: append([]byte(nil), value...)}
}

// Key returns a copy of the message's key.
func (m Message) Key() []byte {
	return append([]byte(nil), m.key...)
}

// Value returns a copy of the message's value.
func (m Message) Value() []byte {
	return append([]byte(nil), m.value...)
}

// Offset returns the queue offset, encoded only in MessageDelivery.
func (m Message) Offset() uint32 {
	return m.offset
}

func (m *Message) toJSON() (messageJSON, error) {
	if err := m.validate(); err != nil {
		return messageJSON{}, err
	}
	key := base64.StdEncoding.EncodeToString(m.key)
	value := base64.StdEncoding.EncodeToString(m.value)
	return messageJSON{Key: &key, Value: &value}, nil
}

func (m *Message) validate() error {
	if m == nil {
		return ErrMissingMessage
	}
	if len(m.key)+len(m.value) > MessageSize {
		return ErrMessageTooLarge
	}
	return nil
}
