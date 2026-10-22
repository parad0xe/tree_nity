package consumer

import "tree_nity/src/server/utils"

// Info stores consumer metadata independently of connection resources.
type Info struct {
	id      id.ID
	offset  uint32
	prefix  string
	ipcPath string
}

func newInfo(id id.ID, offset uint32, prefix string, ipcPath string) *Info {
	return &Info{
		id:      id,
		offset:  offset,
		prefix:  prefix,
		ipcPath: ipcPath,
	}
}

// ID returns the consumer identifier.
func (i *Info) ID() id.ID {
	return i.id
}

// Offset returns the stored message offset.
func (i *Info) Offset() uint32 {
	return i.offset
}

// Prefix returns the subscription prefix.
func (i *Info) Prefix() string {
	return i.prefix
}

// IPCPath returns the name of the consumer's message queue.
func (i *Info) IPCPath() string {
	return i.ipcPath
}

func (i *Info) validate() error {
	if i == nil {
		return ErrInfoUninitialized
	}
	if err := i.id.Validate(); err != nil {
		return err
	}

	return nil
}
