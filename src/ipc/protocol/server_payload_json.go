package protocol

var serverPayloadJSONFactories = map[Opcode]func() payloadJSON{
	ListTopicReply:         func() payloadJSON { return &listTopicReplyPayloadJSON{} },
	CreateTopicReply:       func() payloadJSON { return &createTopicReplyPayloadJSON{} },
	TopicSubscriptionReply: func() payloadJSON { return &topicSubscriptionReplyPayloadJSON{} },
	CheckTopicReply:        func() payloadJSON { return &checkTopicReplyPayloadJSON{} },
	ErrorReply:             func() payloadJSON { return &errorReplyPayloadJSON{} },
}

type listTopicReplyPayloadJSON struct {
	// Topics is required and cannot be null; an empty array is accepted.
	Topics *[]string `json:"topics"`
}

func (p listTopicReplyPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	payload := NewListTopicReplyPayload(*p.Topics)
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p listTopicReplyPayloadJSON) validate() error {
	if p.Topics == nil {
		return ErrMissingTopics
	}
	return nil
}

type createTopicReplyPayloadJSON struct{}

func (p createTopicReplyPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CreateTopicReplyPayload{}, nil
}

func (p createTopicReplyPayloadJSON) validate() error {
	return nil
}

type topicSubscriptionReplyPayloadJSON struct{}

func (p topicSubscriptionReplyPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &TopicSubscriptionReplyPayload{}, nil
}

func (p topicSubscriptionReplyPayloadJSON) validate() error {
	return nil
}

type checkTopicReplyPayloadJSON struct{}

func (p checkTopicReplyPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CheckTopicReplyPayload{}, nil
}

func (p checkTopicReplyPayloadJSON) validate() error {
	return nil
}

type errorReplyPayloadJSON struct {
	// Kind must be present even when its value is InvalidRequest (zero).
	Kind *ErrorKind `json:"kind"`
	// Reason must be present, but an empty string is accepted.
	Reason *string `json:"reason"`
}

func (p errorReplyPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	payload := &ErrorReplyPayload{kind: *p.Kind, reason: *p.Reason}
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p errorReplyPayloadJSON) validate() error {
	if p.Kind == nil {
		return ErrMissingErrorKind
	}
	if p.Reason == nil {
		return ErrMissingErrorReason
	}
	return nil
}
