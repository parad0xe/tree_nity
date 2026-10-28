package protocol

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
