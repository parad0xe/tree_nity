package protocol

import "fmt"

// ErrorKind identifies a request rejection.
type ErrorKind int

const (
	// InvalidRequest marks an invalid request.
	InvalidRequest ErrorKind = iota
	// TopicNotFound marks a missing topic.
	TopicNotFound
	// TopicExists marks a duplicate topic.
	TopicExists
	// ClientNotFound marks an unknown consumer.
	ClientNotFound
	// ClientNameTaken marks an active consumer name.
	ClientNameTaken
)

func (k ErrorKind) validate() error {
	switch k {
	case InvalidRequest, TopicNotFound, TopicExists, ClientNotFound, ClientNameTaken:
		return nil
	}
	return fmt.Errorf("%w: unknown error kind %d", ErrInvalidPayload, k)
}
