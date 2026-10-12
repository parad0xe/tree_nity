package session

type Consumer struct {
	AcknowledgedChan chan struct{}
	HeartbeatChan    chan struct{}
}
