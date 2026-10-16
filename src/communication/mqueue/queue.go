package mqueue

/*
#include <stdlib.h>
#include <mqueue.h>

static mqd_t    mq_open_wrapper(const char *name, int oflag, mode_t mode, struct mq_attr *attr) {
    return mq_open(name, oflag, mode, attr);
}
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

// Mode is the access mode of an opened queue.
type Mode int

const (
	// Read opens the queue to receive messages.
	Read Mode = C.O_RDONLY
	// Write opens the queue to send messages.
	Write Mode = C.O_WRONLY
)

// Option changes how Open behaves. Options can be combined with |.
type Option int

const (
	// Create creates the queue when it does not exist.
	Create Option = C.O_CREAT
	// Exclusive makes Open fail with os.ErrExist when Create finds an
	// existing queue.
	Exclusive Option = C.O_EXCL
	// NonBlock makes operations fail with syscall.EAGAIN instead of waiting
	// on a full or empty queue.
	NonBlock Option = C.O_NONBLOCK
)

// Queue is an open descriptor on a named POSIX message queue.
type Queue struct {
	name string
	mqd  C.mqd_t
}

// Open opens the queue name with the given mode and options. maxMsg and
// msgSize set the capacity of a queue created with Create and are ignored
// otherwise.
func Open(name string, mode Mode, opts Option, maxMsg int64, msgSize int64) (*Queue, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	attr := C.struct_mq_attr{mq_maxmsg: C.long(maxMsg), mq_msgsize: C.long(msgSize)}
	mqd, err := C.mq_open_wrapper(cname, C.int(mode)|C.int(opts), C.mode_t(0o600), &attr)
	if mqd == -1 {
		return nil, fmt.Errorf("open queue %q: %w", name, err)
	}

	return &Queue{
		name: name,
		mqd:  mqd,
	}, nil
}

// Close releases the descriptor. The queue stays available to other
// processes until it is unlinked.
func (q *Queue) Close() error {
	if retcode, err := C.mq_close(q.mqd); retcode == -1 {
		return fmt.Errorf("close queue %q: %w", q.name, err)
	}

	return nil
}

// Unlink removes the queue name from the system. Open descriptors remain
// usable until they are closed.
func (q *Queue) Unlink() error {
	cname := C.CString(q.name)
	defer C.free(unsafe.Pointer(cname))

	if retcode, err := C.mq_unlink(cname); retcode == -1 {
		return fmt.Errorf("unlink queue %q: %w", q.name, err)
	}

	return nil
}

// Drop closes the descriptor and removes the queue. It attempts both
// operations even when the first one fails.
func (q *Queue) Drop() error {
	return errors.Join(q.Close(), q.Unlink())
}
