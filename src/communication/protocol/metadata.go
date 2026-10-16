package protocol

type Metadata interface {
}

type ClientMetadata struct {
	clientId  string
	clientIPC string
}

type ServerMetadata struct {
}
