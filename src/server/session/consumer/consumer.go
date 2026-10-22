package consumer

// Consumer combines stored metadata with the resources for one connection.
type Consumer struct {
	Info       Info
	Connection *Connection
}

func newConsumer(info *Info, connection *Connection) *Consumer {
	return &Consumer{
		Info:       *info,
		Connection: connection,
	}
}

// Validate checks the consumer, its metadata, and its connection.
// It returns ErrUninitialized for a nil consumer, or the metadata or connection error.
func (c *Consumer) Validate() error {
	if c == nil {
		return ErrUninitialized
	}
	if err := c.Info.validate(); err != nil {
		return err
	}
	if err := c.Connection.validate(); err != nil {
		return err
	}
	return nil
}
