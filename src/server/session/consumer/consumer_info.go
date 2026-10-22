package consumer

import (
	id "tree_nity/src/server/utils"
)

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

func (i *Info) ID() id.ID {
	return i.id
}

func (i *Info) Offset() uint32 {
	return i.offset
}

func (i *Info) Prefix() string {
	return i.prefix
}

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
