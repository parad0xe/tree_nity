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
	"os"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

// queue is an open descriptor on a named POSIX message queue.
type queue struct {
	name      string
	mqd       C.mqd_t
	frameSize int
	maxFrame  int
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

// openMqueue opens the queue name with the given flags and reads its attributes.
func openMqueue(name string, flags int, attr *C.struct_mq_attr) (*queue, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	mqd, err := C.mq_open_wrapper(cname, C.int(flags), C.mode_t(0o600), attr)
	if mqd == -1 {
		return nil, fmt.Errorf("open queue %q: %w", name, err)
	}

	q := &queue{
		name: name,
		mqd:  mqd,
	}

	retrievedAttr := C.struct_mq_attr{}
	if retcode, err := C.mq_getattr(mqd, &retrievedAttr); retcode == -1 {
		return nil, errors.Join(fmt.Errorf("open queue %q: %w", name, err), q.close())
	}

	q.frameSize = int(retrievedAttr.mq_msgsize)
	q.maxFrame = int(retrievedAttr.mq_maxmsg)

	return q, nil
}

// createMqueue creates the queue name and opens it to receive frames.
func createMqueue(name string) (*queue, error) {
	attr := C.struct_mq_attr{mq_maxmsg: C.long(maxFrame), mq_msgsize: C.long(frameSize)}
	return openMqueue(name, C.O_RDONLY|C.O_CREAT|C.O_EXCL, &attr)
}

// connectMqueue opens the existing queue name to send frames.
func connectMqueue(name string) (*queue, error) {
	return openMqueue(name, C.O_WRONLY, nil)
}

// recv blocks until a frame is available and returns its content.
func (q *queue) recv() ([]byte, error) {
	buffer := make([]byte, q.frameSize)

	cbuffer := unsafe.Pointer(&buffer[0])
	size, err := C.mq_receive(q.mqd, (*C.char)(cbuffer), C.size_t(len(buffer)), nil)
	if size == -1 {
		return nil, fmt.Errorf("receive from queue %q: %w", q.name, err)
	}

	return buffer[:size], nil
}

// toTimespec converts deadline into the absolute time taken by the timed queue
// operations.
func toTimespec(deadline time.Time) C.struct_timespec {
	var ts C.struct_timespec
	ts.tv_sec = C.time_t(deadline.Unix())
	ts.tv_nsec = C.long(deadline.Nanosecond())

	return ts
}

// timedRecv waits until deadline for a frame and returns its content. A past
// deadline only returns a frame already waiting.
func (q *queue) timedRecv(deadline time.Time) ([]byte, error) {
	buffer := make([]byte, q.frameSize)

	ts := toTimespec(deadline)

	cbuffer := unsafe.Pointer(&buffer[0])
	size, err := C.mq_timedreceive(q.mqd, (*C.char)(cbuffer), C.size_t(len(buffer)), nil, &ts)
	if size == -1 {
		if errors.Is(err, syscall.ETIMEDOUT) {
			return nil, fmt.Errorf("timed receive from queue %q: %w", q.name, os.ErrDeadlineExceeded)
		}
		return nil, fmt.Errorf("timed receive from queue %q: %w", q.name, err)
	}

	return buffer[:size], nil
}

// checkFrame returns an error matching ErrEmptyFrame when frame is empty, and
// ErrFrameTooLarge when it exceeds the frame size of the queue.
func (q *queue) checkFrame(frame []byte) error {
	if len(frame) == 0 {
		return ErrEmptyFrame
	}

	if len(frame) > q.frameSize {
		return fmt.Errorf("%w: %d bytes, maximum %d", ErrFrameTooLarge, len(frame), q.frameSize)
	}

	return nil
}

// send blocks while the queue is full, then sends frame with the given
// priority. The returned error matches ErrEmptyFrame when frame is empty, and
// ErrFrameTooLarge when it exceeds the frame size of the queue.
func (q *queue) send(frame []byte, framePriority priority) error {
	if err := q.checkFrame(frame); err != nil {
		return fmt.Errorf("send to queue %q: %w", q.name, err)
	}

	cbuffer := unsafe.Pointer(&frame[0])
	retcode, err := C.mq_send(q.mqd, (*C.char)(cbuffer), C.size_t(len(frame)), C.uint(framePriority))
	if retcode == -1 {
		return fmt.Errorf("send to queue %q: %w", q.name, err)
	}

	return nil
}

// timedSend waits until deadline for room in the queue, then sends frame with
// the given priority. A past deadline only sends when the queue has room. The
// zero time is not a valid deadline. The returned error matches
// os.ErrDeadlineExceeded when the queue stays full until deadline,
// ErrEmptyFrame when frame is empty, and ErrFrameTooLarge when it exceeds the
// frame size of the queue.
func (q *queue) timedSend(frame []byte, framePriority priority, deadline time.Time) error {
	if err := q.checkFrame(frame); err != nil {
		return fmt.Errorf("timed send to queue %q: %w", q.name, err)
	}

	ts := toTimespec(deadline)

	cbuffer := unsafe.Pointer(&frame[0])
	retcode, err := C.mq_timedsend(q.mqd, (*C.char)(cbuffer), C.size_t(len(frame)), C.uint(framePriority), &ts)
	if retcode == -1 {
		if errors.Is(err, syscall.ETIMEDOUT) {
			return fmt.Errorf("timed send to queue %q: %w", q.name, os.ErrDeadlineExceeded)
		}

		return fmt.Errorf("timed send to queue %q: %w", q.name, err)
	}

	return nil
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
