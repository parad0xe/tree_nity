package mqueue

import "time"

// Sender sends frames to an existing queue.
type Sender struct {
	queue *queue
}

// NewSender opens the existing queue name to send frames. The returned error
// matches os.ErrNotExist when the queue does not exist.
func NewSender(name string) (*Sender, error) {
	q, err := connectMqueue(name)
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

// SendAhead blocks while the queue is full, then sends frame ahead of the
// frames already waiting.
func (s *Sender) SendAhead(frame []byte) error {
	return s.queue.send(frame, priorityHigh)
}

// TimedSend waits until deadline for room in the queue, then sends frame
// behind the frames already waiting. A past deadline only sends when the queue
// has room. The zero time is not a valid deadline. The returned error matches
// os.ErrDeadlineExceeded when the queue stays full until deadline.
func (s *Sender) TimedSend(frame []byte, deadline time.Time) error {
	return s.queue.timedSend(frame, priorityNormal, deadline)
}

// TimedSendAhead waits until deadline for room in the queue, then sends frame
// ahead of the frames already waiting. A past deadline only sends when the
// queue has room. The zero time is not a valid deadline. The returned error
// matches os.ErrDeadlineExceeded when the queue stays full until deadline.
func (s *Sender) TimedSendAhead(frame []byte, deadline time.Time) error {
	return s.queue.timedSend(frame, priorityHigh, deadline)
}

// Close closes the connection. The queue stays available to its owner.
func (s *Sender) Close() error {
	return s.queue.close()
}
