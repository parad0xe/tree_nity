package protocol

import "fmt"

// ErrorKind identifies why the server rejected a request.
type ErrorKind int

const (
	// InvalidRequest identifies an invalid field or an unexpected operation.
	InvalidRequest ErrorKind = iota
	// TopicNotFound identifies a request for a topic that does not exist.
	TopicNotFound
	// TopicExists identifies a request to create a topic that already exists.
	TopicExists
	// ClientNotFound identifies a request for an unknown consumer.
	ClientNotFound
	// ClientNameTaken identifies a subscription using an active consumer's name.
	ClientNameTaken
)

func (k ErrorKind) validate() error {
	switch k {
	case InvalidRequest, TopicNotFound, TopicExists, ClientNotFound, ClientNameTaken:
		return nil
	}
	return fmt.Errorf("%w: unknown error kind %d", ErrInvalidPayload, k)
}
