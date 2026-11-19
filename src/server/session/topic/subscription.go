package topic

import (
	"context"
	"fmt"

	"tree_nity/src/server/session/consumer"
)

// subscription snapshots metadata and a connection for one pair of workers.
type subscription struct {
	info           consumer.Info
	connection     *consumer.Connection
	cancel         context.CancelFunc
	onAck          func(uint32) error
	awaitingACK    bool
	expectedOffset uint32
}

func (s *subscription) validate() error {
	if s == nil || s.connection == nil || s.connection.Done() == nil {
		return ErrInvalidConsumer
	}
	if err := s.info.ID().Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConsumer, err)
	}
	return nil
}
