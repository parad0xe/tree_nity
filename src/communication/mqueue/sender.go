package mqueue

// Sender sends frames to an existing queue.
type Sender struct {
	queue *queue
}

// NewSender opens the existing queue name to send frames. The returned error
// matches os.ErrNotExist when the queue does not exist.
func NewSender(name string) (*Sender, error) {
	q, err := connect(name)
	if err != nil {
		return nil, err
	}

	return &Sender{
		queue: q,
	}, nil
}

// Send blocks while the queue is full, then sends frame.
func (s *Sender) Send(frame []byte) error {
	return s.queue.send(frame)
}

// Close closes the connection. The queue stays available to its owner.
func (s *Sender) Close() error {
	return s.queue.close()
}
