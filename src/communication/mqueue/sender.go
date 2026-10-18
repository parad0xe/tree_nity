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

// Send blocks while the queue is full, then sends frame behind the frames
// already waiting.
func (s *Sender) Send(frame []byte) error {
	return s.queue.send(frame, priorityNormal)
}

// SendUrgent blocks while the queue is full, then sends frame ahead of the
// frames already waiting.
func (s *Sender) SendUrgent(frame []byte) error {
	return s.queue.send(frame, priorityHigh)
}

// Close closes the connection. The queue stays available to its owner.
func (s *Sender) Close() error {
	return s.queue.close()
}
