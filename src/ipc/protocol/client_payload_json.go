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

type createTopicPayloadJSON struct{}

func (p createTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CreateTopicPayload{}, nil
}

func (p createTopicPayloadJSON) validate() error {
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

type topicSubscriptionPayloadJSON struct{}

func (p topicSubscriptionPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &TopicSubscriptionPayload{}, nil
}

func (p topicSubscriptionPayloadJSON) validate() error {
	return nil
}

type acknowledgementPayloadJSON struct{}

func (p acknowledgementPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &AcknowledgementPayload{}, nil
}

func (p acknowledgementPayloadJSON) validate() error {
	return nil
}

type checkTopicPayloadJSON struct{}

func (p checkTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CheckTopicPayload{}, nil
}

func (p checkTopicPayloadJSON) validate() error {
	return nil
}

type produceMessagePayloadJSON struct {
	// Raw must be present even when its value is false.
	Raw *bool `json:"raw"`
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

	return &ProduceMessagePayload{
		raw:     *p.Raw,
		message: message,
	}, nil
}

func (p produceMessagePayloadJSON) validate() error {
	if p.Raw == nil {
		return ErrMissingRaw
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
