package protocol

var serverPayloadJSONFactories = map[Opcode]func() payloadJSON{
	ErrorReply: func() payloadJSON { return &errorReplyPayloadJSON{} },
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
