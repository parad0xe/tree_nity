package mqueue

/*
#include <limits.h>
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
	"strings"
	"unsafe"
)

// mode is the access mode of an opened queue.
type mode int

const (
	// Read opens the queue to receive messages.
	Read mode = C.O_RDONLY
	// Write opens the queue to send messages.
	Write mode = C.O_WRONLY
)

// option changes how open behaves. Options can be combined with |.
type option int

const (
	// Create creates the queue when it does not exist.
	Create option = C.O_CREAT
	// Exclusive makes open fail with os.ErrExist when Create finds an
	// existing queue.
	Exclusive option = C.O_EXCL
	// NonBlock makes operations fail with syscall.EAGAIN instead of waiting
	// on a full or empty queue.
	NonBlock option = C.O_NONBLOCK
)

// queue is an open descriptor on a named POSIX message queue.
type queue struct {
	name string
	mqd  C.mqd_t
}

// nameMax is the maximum length, in bytes, of a queue name after its leading "/".
const nameMax = C.NAME_MAX

// validateName returns an error wrapping ErrInvalidName unless name is a "/"
// followed by 1 to nameMax bytes that contain no other "/".
func validateName(name string) error {
	if !strings.HasPrefix(name, "/") {
		return fmt.Errorf(`%w %q: must start with "/"`, ErrInvalidName, name)
	}

	if strings.Count(name, "/") > 1 {
		return fmt.Errorf(`%w %q: must not contain "/" after the first character`, ErrInvalidName, name)
	}

	if len(name) < 2 {
		return fmt.Errorf(`%w %q: must have a character after "/"`, ErrInvalidName, name)
	}

	if len(name) > nameMax+1 {
		return fmt.Errorf(`%w %q: must not exceed %d bytes after "/"`, ErrInvalidName, name, nameMax)
	}

	return nil
}

// open opens the queue name with the given mode and options. maxMsg and
// msgSize set the capacity of a queue created with Create and are ignored
// otherwise.
func open(name string, mode mode, opts option, maxMsg int64, msgSize int64) (*queue, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	attr := C.struct_mq_attr{mq_maxmsg: C.long(maxMsg), mq_msgsize: C.long(msgSize)}
	mqd, err := C.mq_open_wrapper(cname, C.int(mode)|C.int(opts), C.mode_t(0o600), &attr)
	if mqd == -1 {
		return nil, fmt.Errorf("open queue %q: %w", name, err)
	}

	return &queue{
		name: name,
		mqd:  mqd,
	}, nil
}

// close releases the descriptor. The queue stays available to other
// processes until it is unlinked.
func (q *queue) close() error {
	if retcode, err := C.mq_close(q.mqd); retcode == -1 {
		return fmt.Errorf("close queue %q: %w", q.name, err)
	}

	return nil
}

// unlink removes the queue name from the system. open descriptors remain
// usable until they are closed.
func (q *queue) unlink() error {
	cname := C.CString(q.name)
	defer C.free(unsafe.Pointer(cname))

	if retcode, err := C.mq_unlink(cname); retcode == -1 {
		return fmt.Errorf("unlink queue %q: %w", q.name, err)
	}

	return nil
}

// drop closes the descriptor and removes the queue. It attempts both
// operations even when the first one fails.
func (q *queue) drop() error {
	return errors.Join(q.close(), q.unlink())
}
