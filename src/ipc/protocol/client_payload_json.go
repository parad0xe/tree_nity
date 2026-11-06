package protocol

var clientPayloadJSONFactories = map[Opcode]func() payloadJSON{
	ListTopic:          func() payloadJSON { return &listTopicPayloadJSON{} },
	CreateTopic:        func() payloadJSON { return &createTopicPayloadJSON{} },
	ConsumerInfo:       func() payloadJSON { return &consumerInfoPayloadJSON{} },
	TopicSubscription:  func() payloadJSON { return &topicSubscriptionPayloadJSON{} },
	Acknowledgement:    func() payloadJSON { return &acknowledgementPayloadJSON{} },
	CheckTopic:         func() payloadJSON { return &checkTopicPayloadJSON{} },
	ProduceMessage:     func() payloadJSON { return &produceMessagePayloadJSON{} },
	ConsumerDisconnect: func() payloadJSON { return &consumerDisconnectPayloadJSON{} },
	ConsumerHeartbeat:  func() payloadJSON { return &consumerHeartbeatPayloadJSON{} },
}

type listTopicPayloadJSON struct{}

func (p listTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ListTopicPayload{}, nil
}

func (p listTopicPayloadJSON) validate() error {
	return nil
}

type createTopicPayloadJSON struct {
	// Topic must match ^[a-zA-Z0-9_.-]{1,32}$.
	Topic *string `json:"topic"`
}

func (p createTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	payload := &CreateTopicPayload{topic: *p.Topic}
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p createTopicPayloadJSON) validate() error {
	if p.Topic == nil {
		return ErrMissingTopic
	}
	return nil
}

type consumerInfoPayloadJSON struct {
	// Subscriber is the required consumer name to query.
	Subscriber *string `json:"subscriber"`
}

func (p consumerInfoPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	payload := &ConsumerInfoPayload{subscriber: *p.Subscriber}
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p consumerInfoPayloadJSON) validate() error {
	if p.Subscriber == nil {
		return ErrMissingSubscriber
	}
	return nil
}

type topicSubscriptionPayloadJSON struct {
	// Topic must match ^[a-zA-Z0-9_.-]{1,32}$.
	Topic *string `json:"topic"`
	// Prefix is required; empty matches all messages.
	Prefix *string `json:"prefix"`
	// Offset is optional; absent or null resumes, zero replays.
	Offset *uint32 `json:"offset,omitempty"`
}

func (p topicSubscriptionPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	payload := NewTopicSubscriptionPayload(*p.Topic, *p.Prefix, p.Offset)
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p topicSubscriptionPayloadJSON) validate() error {
	if p.Topic == nil {
		return ErrMissingTopic
	}
	if p.Prefix == nil {
		return ErrMissingPrefix
	}
	return nil
}

type acknowledgementPayloadJSON struct {
	// Offset is required, including zero.
	Offset *uint32 `json:"offset"`
}

func (p acknowledgementPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	payload := &AcknowledgementPayload{offset: *p.Offset}
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p acknowledgementPayloadJSON) validate() error {
	if p.Offset == nil {
		return ErrMissingOffset
	}
	return nil
}

type checkTopicPayloadJSON struct {
	// Topic must match ^[a-zA-Z0-9_.-]{1,32}$.
	Topic *string `json:"topic"`
}

func (p checkTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	payload := &CheckTopicPayload{topic: *p.Topic}
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p checkTopicPayloadJSON) validate() error {
	if p.Topic == nil {
		return ErrMissingTopic
	}
	return nil
}

type produceMessagePayloadJSON struct {
	// Topic must match ^[a-zA-Z0-9_.-]{1,32}$.
	Topic *string `json:"topic"`
	// Message is required and limited to MessageSize bytes.
	Message *messageJSON `json:"message"`
}

func (p produceMessagePayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}

	message, err := p.Message.toMessage()
	if err != nil {
		return nil, err
	}

	payload := &ProduceMessagePayload{topic: *p.Topic, message: message}
	if err := payload.validate(); err != nil {
		return nil, err
	}
	return payload, nil
}

func (p produceMessagePayloadJSON) validate() error {
	if p.Topic == nil {
		return ErrMissingTopic
	}
	if p.Message == nil {
		return ErrMissingMessage
	}
	return nil
}

type consumerDisconnectPayloadJSON struct{}

func (p consumerDisconnectPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerDisconnectPayload{}, nil
}

func (p consumerDisconnectPayloadJSON) validate() error {
	return nil
}

type consumerHeartbeatPayloadJSON struct{}

func (p consumerHeartbeatPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerHeartbeatPayload{}, nil
}

func (p consumerHeartbeatPayloadJSON) validate() error {
	return nil
}
