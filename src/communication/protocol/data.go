package protocol

type Data struct {
	opcode   op
	emitter  emitter
	metadata Metadata
	payload  Payload
}

type dataJSON struct {
	Opcode   op       `json:"opcode"`
	Emitter  emitter  `json:"emitter"`
	Metadata Metadata `json:"metadata"`
	Payload  Payload  `json:"payload"`
}

func (d Data) parse() error {

	return nil
}
