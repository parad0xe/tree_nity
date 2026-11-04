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
	// Topic is required and must match the topic name format.
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

type consumerInfoPayloadJSON struct{}

func (p consumerInfoPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerInfoPayload{}, nil
}

func (p consumerInfoPayloadJSON) validate() error {
	return nil
}

type topicSubscriptionPayloadJSON struct {
	// Topic is required and must match the topic name format.
	Topic *string `json:"topic"`
	// Prefix is required, but an empty string matches all messages.
	Prefix *string `json:"prefix"`
	// Offset is optional; nil requests resume, while explicit zero requests replay.
	Offset *int32 `json:"offset,omitempty"`
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
	// Offset is required even when its value is zero.
	Offset *int32 `json:"offset"`
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
	// Topic is required and must match the topic name format.
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
	// Topic is required and must match the topic name format.
	Topic *string `json:"topic"`
	// Message must be present and pass key, value, and size validation.
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
