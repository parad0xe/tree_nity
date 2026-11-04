package protocol

var serverPayloadJSONFactories = map[Opcode]func() payloadJSON{
	ListTopicReply:         func() payloadJSON { return &listTopicReplyPayloadJSON{} },
	CreateTopicReply:       func() payloadJSON { return &createTopicReplyPayloadJSON{} },
	ConsumerInfoReply:      func() payloadJSON { return &consumerInfoReplyPayloadJSON{} },
	TopicSubscriptionReply: func() payloadJSON { return &topicSubscriptionReplyPayloadJSON{} },
	CheckTopicReply:        func() payloadJSON { return &checkTopicReplyPayloadJSON{} },
	ErrorReply:             func() payloadJSON { return &errorReplyPayloadJSON{} },
	MessageDelivery:        func() payloadJSON { return &messageDeliveryPayloadJSON{} },
	ServerShutdown:         func() payloadJSON { return &serverShutdownPayloadJSON{} },
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

// Field order matches the consumer info command's output order.
type consumerInfoReplyPayloadJSON struct {
	Client *string `json:"client"`
	Topic  *string `json:"topic"`
	// Offset is required even when zero.
	Offset *int32 `json:"offset"`
	// Prefix is required even when empty.
	Prefix *string `json:"prefix"`
	// IPC is required even when empty.
	IPC *string `json:"ipc"`
}

func (p consumerInfoReplyPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	payload := NewConsumerInfoReplyPayload(*p.Client, *p.Topic, *p.Offset, *p.Prefix, *p.IPC)
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p consumerInfoReplyPayloadJSON) validate() error {
	if p.Client == nil {
		return ErrMissingClient
	}
	if p.Topic == nil {
		return ErrMissingTopic
	}
	if p.Offset == nil {
		return ErrMissingOffset
	}
	if p.Prefix == nil {
		return ErrMissingPrefix
	}
	if p.IPC == nil {
		return ErrMissingIPC
	}
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

type deliveredMessageJSON struct {
	messageJSON
	// Offset is required even when zero; production messages use messageJSON alone.
	Offset *int32 `json:"offset"`
}

type messageDeliveryPayloadJSON struct {
	// Message must be present and include a queue offset.
	Message *deliveredMessageJSON `json:"message"`
}

func (p messageDeliveryPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	message, err := p.Message.messageJSON.toMessage()
	if err != nil {
		return nil, err
	}
	payload := NewMessageDeliveryPayload(*p.Message.Offset, message)
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p messageDeliveryPayloadJSON) validate() error {
	if p.Message == nil {
		return ErrMissingMessage
	}
	if p.Message.Offset == nil {
		return ErrMissingMessageOffset
	}
	return nil
}

type serverShutdownPayloadJSON struct{}

func (p serverShutdownPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ServerShutdownPayload{}, nil
}

func (p serverShutdownPayloadJSON) validate() error {
	return nil
}
