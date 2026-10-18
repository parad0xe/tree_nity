package mqueue

// Receiver owns a POSIX message queue and receives the frames sent to it.
type Receiver struct {
	queue *queue
}

// NewReceiver creates the queue name and opens it to receive frames. The
// returned error matches os.ErrExist when the queue already exists.
func NewReceiver(name string) (*Receiver, error) {
	q, err := create(name)
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

// Drop closes the queue and removes it from the system.
func (r *Receiver) Drop() error {
	return r.queue.drop()
}
