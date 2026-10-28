package protocol

type payloadJSON interface {
	validate() error
	toPayload() (Payload, error)
}

var payloadJSONFactories = map[Emitter]map[Opcode]func() payloadJSON{
	Client: clientPayloadJSONFactories,
	Server: serverPayloadJSONFactories,
}
