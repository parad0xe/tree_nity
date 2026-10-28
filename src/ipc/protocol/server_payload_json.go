package protocol

var serverPayloadJSONFactories = map[Opcode]func() payloadJSON{}
