package lifecycle

// GracefulShutdown tells whether a shutdown was requested.
type GracefulShutdown struct {
	requestedChan <-chan struct{}
}

// NewGracefulShutdown returns a GracefulShutdown requested once requestedChan
// is closed.
func NewGracefulShutdown(requestedChan <-chan struct{}) GracefulShutdown {
	return GracefulShutdown{
		requestedChan: requestedChan,
	}
}

// Requested reports, without waiting, whether the shutdown was requested.
func (s GracefulShutdown) Requested() bool {
	select {
	case <-s.requestedChan:
		return true
	default:
		return false
	}
}
