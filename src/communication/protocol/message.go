package protocol

import (
	"errors"
	"fmt"
)

type Message struct {
	offset int32
	key    string
	value  string
}

type messageJSON struct {
	Key   *string `json:"key"`
	Value *string `json:"value"`
}

func (m messageJSON) validate() error {
	if m.Key == nil {
		return errors.New("missing message key")
	}
	if m.Value == nil {
		return errors.New("missing message value")
	}
	return nil
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

func (m Message) validate() error {
	if len(m.key)+len(m.value) > MessageSize {
		return fmt.Errorf("message exceeds %d bytes", MessageSize)
	}
	return nil
}
