package protocol

import (
	"fmt"
	"unicode/utf8"

	"tree_nity/src/ipc/id"
)

// ListTopicReplyPayload carries topic names in their supplied order.
// Its JSON representation is {"topics":["<name>"]}; an empty list is encoded as [].
// Topic names follow the same format as CreateTopicPayload.
type ListTopicReplyPayload struct {
	topics []string
}

// NewListTopicReplyPayload returns a response containing a copy of topics.
// Nil and empty slices both represent an empty topic list.
func NewListTopicReplyPayload(topics []string) *ListTopicReplyPayload {
	return &ListTopicReplyPayload{topics: append([]string(nil), topics...)}
}

// Topics returns a copy of the topic names in their supplied order.
func (p ListTopicReplyPayload) Topics() []string {
	return append([]string(nil), p.topics...)
}

func (p *ListTopicReplyPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != ListTopicReply {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	topics := p.topics
	if topics == nil {
		topics = []string{}
	}
	return &listTopicReplyPayloadJSON{Topics: &topics}, nil
}

func (p *ListTopicReplyPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	for i, topic := range p.topics {
		if err := id.ID(topic).Validate(); err != nil {
			return fmt.Errorf("%w: invalid topic at index %d: %q", ErrInvalidPayload, i, topic)
		}
	}
	return nil
}

// CreateTopicReplyPayload confirms topic creation with an empty JSON object.
// A duplicate topic is reported with ErrorReply and TopicExists instead.
type CreateTopicReplyPayload struct{}

// NewCreateTopicReplyPayload returns an empty topic creation response.
func NewCreateTopicReplyPayload() *CreateTopicReplyPayload {
	return &CreateTopicReplyPayload{}
}

func (p *CreateTopicReplyPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != CreateTopicReply {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &createTopicReplyPayloadJSON{}, nil
}

func (p *CreateTopicReplyPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	return nil
}

// ErrorReplyPayload carries a rejection kind and a reason for the client.
type ErrorReplyPayload struct {
	kind   ErrorKind
	reason string
}

// NewErrorReplyPayload returns a payload describing a rejected request.
func NewErrorReplyPayload(kind ErrorKind, reason string) *ErrorReplyPayload {
	return &ErrorReplyPayload{kind: kind, reason: reason}
}

// Kind returns the reason's machine-readable category.
func (p ErrorReplyPayload) Kind() ErrorKind {
	return p.kind
}

// Reason returns the diagnostic for the client to display.
func (p ErrorReplyPayload) Reason() string {
	return p.reason
}

func (p *ErrorReplyPayload) toJSON(emitter Emitter, opcode Opcode) (payloadJSON, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	if emitter != Server || opcode != ErrorReply {
		return nil, fmt.Errorf("%w for emitter %d and opcode %d: %T", ErrInvalidPayload, emitter, opcode, p)
	}
	return &errorReplyPayloadJSON{Kind: &p.kind, Reason: &p.reason}, nil
}

func (p *ErrorReplyPayload) validate() error {
	if p == nil {
		return ErrMissingPayload
	}
	if err := p.kind.validate(); err != nil {
		return err
	}
	if !utf8.ValidString(p.reason) {
		return fmt.Errorf("%w: error reason must contain valid UTF-8", ErrInvalidPayload)
	}
	return nil
}
