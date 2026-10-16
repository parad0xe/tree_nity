package protocol

import "errors"

type Payload interface {
	parse() error
}

type payloadJSON interface {
	validate() error
	toPayload() (Payload, error)
}

type ListTopicPayload struct{}
type listTopicPayloadJSON struct{}

func (p listTopicPayloadJSON) validate() error {
	return nil
}

func (p listTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ListTopicPayload{}, nil
}

func (p *ListTopicPayload) parse() error {
	return nil
}

type CreateTopicPayload struct{}
type createTopicPayloadJSON struct{}

func (p createTopicPayloadJSON) validate() error {
	return nil
}

func (p createTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CreateTopicPayload{}, nil
}

func (p *CreateTopicPayload) parse() error {
	return nil
}

type ConsumerInfoPayload struct{}
type consumerInfoPayloadJSON struct{}

func (p consumerInfoPayloadJSON) validate() error {
	return nil
}

func (p consumerInfoPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerInfoPayload{}, nil
}

func (p *ConsumerInfoPayload) parse() error {
	return nil
}

type TopicSubscriptionPayload struct{}
type topicSubscriptionPayloadJSON struct{}

func (p topicSubscriptionPayloadJSON) validate() error {
	return nil
}

func (p topicSubscriptionPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &TopicSubscriptionPayload{}, nil
}

func (p *TopicSubscriptionPayload) parse() error {
	return nil
}

type AcknowledgementPayload struct{}
type acknowledgementPayloadJSON struct{}

func (p acknowledgementPayloadJSON) validate() error {
	return nil
}

func (p acknowledgementPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &AcknowledgementPayload{}, nil
}

func (p *AcknowledgementPayload) parse() error {
	return nil
}

type CheckTopicPayload struct{}
type checkTopicPayloadJSON struct{}

func (p checkTopicPayloadJSON) validate() error {
	return nil
}

func (p checkTopicPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &CheckTopicPayload{}, nil
}

func (p *CheckTopicPayload) parse() error {
	return nil
}

type ProduceMessagePayload struct {
	raw     bool
	message Message
}

type produceMessagePayloadJSON struct {
	Raw     *bool        `json:"raw"`
	Message *messageJSON `json:"message"`
}

func (p produceMessagePayloadJSON) validate() error {
	if p.Raw == nil {
		return errors.New("missing raw")
	}
	if p.Message == nil {
		return errors.New("missing message")
	}
	return nil
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

func (p *ProduceMessagePayload) parse() error {
	return p.message.validate()
}

type ConsumerDisconnectPayload struct{}
type consumerDisconnectPayloadJSON struct{}

func (p consumerDisconnectPayloadJSON) validate() error {
	return nil
}

func (p consumerDisconnectPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerDisconnectPayload{}, nil
}

func (p *ConsumerDisconnectPayload) parse() error {
	return nil
}

type ConsumerHeartbeatPayload struct{}
type consumerHeartbeatPayloadJSON struct{}

func (p consumerHeartbeatPayloadJSON) validate() error {
	return nil
}

func (p consumerHeartbeatPayloadJSON) toPayload() (Payload, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &ConsumerHeartbeatPayload{}, nil
}

func (p *ConsumerHeartbeatPayload) parse() error {
	return nil
}
