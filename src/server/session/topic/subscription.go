package topic

import (
	"context"
	"fmt"
	"time"

	"tree_nity/src/server/session/consumer"
)

// consumerTimeout is the maximum inactivity after confirmation or an accepted notification.
const consumerTimeout = 30 * time.Second

// subscription snapshots metadata and a connection for one pair of workers.
type subscription struct {
	info           consumer.Info
	connection     *consumer.Connection
	cancel         context.CancelFunc
	onAck          func(uint32) error
	awaitingACK    bool
	expectedOffset uint32
	liveness       *consumerLiveness
	// done closes after both workers finish cleanup, including onStop.
	done chan struct{}
}

// consumerLiveness is shared by the stored subscription and its workers.
// The topic mutex protects deadline and expired.
type consumerLiveness struct {
	changed  chan struct{}
	deadline time.Time
	expired  bool
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
