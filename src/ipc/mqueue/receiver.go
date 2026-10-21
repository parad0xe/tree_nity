package mqueue

import "time"

// Receiver owns a POSIX message queue and receives the frames sent to it.
type Receiver struct {
	queue *queue
}

// NewReceiver creates the queue name and opens it to receive frames. The
// returned error matches os.ErrExist when the queue already exists.
func NewReceiver(name string) (*Receiver, error) {
	q, err := createMqueue(name)
	if err != nil {
		return nil, err
	}

	return &Receiver{
		queue: q,
	}, nil
}

// Recv blocks until a frame is available and returns its content.
func (r *Receiver) Recv() ([]byte, error) {
	return r.queue.recv()
}

// TimedRecv waits until deadline for a frame and returns its content. A past
// deadline only returns a frame already waiting. The zero time is not a valid
// deadline. The returned error matches os.ErrDeadlineExceeded when no frame
// arrives in time.
func (r *Receiver) TimedRecv(deadline time.Time) ([]byte, error) {
	return r.queue.timedRecv(deadline)
}

// Drop closes the queue and removes it from the system.
func (r *Receiver) Drop() error {
	return r.queue.drop()
}
