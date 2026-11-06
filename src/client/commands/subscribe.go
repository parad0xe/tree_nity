package commands

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/id"
)

type subscribeCommand struct {
	topic      id.ID
	subscriber id.ID
	prefix     string
	offset     *uint32
	raw        bool
}

// ParseSubscribe parses the arguments of the subscribe command.
func ParseSubscribe(args []string) (Command, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("subscribe: %w: <topic_name>", errMissingArgument)
	}
	topicID := id.ID(args[0])
	if err := topicID.Validate(); err != nil {
		return nil, fmt.Errorf("subscribe: topic %q: %w", topicID, err)
	}

	if len(args) < 2 {
		return nil, fmt.Errorf("subscribe: %w: <subscriber_name>", errMissingArgument)
	}
	subscriber := id.ID(args[1])
	if err := subscriber.Validate(); err != nil {
		return nil, fmt.Errorf("subscribe: subscriber %q: %w", subscriber, err)
	}

	cmd := &subscribeCommand{
		topic:      topicID,
		subscriber: subscriber,
	}

	fs := newFlagSet("subscribe")
	fs.Func("offset", "start from this message position", func(value string) error {
		offset, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return err
		}
		start := uint32(offset)
		cmd.offset = &start
		return nil
	})
	fs.StringVar(&cmd.prefix, "prefix", "", "only receive messages whose key has this prefix")
	fs.BoolVar(&cmd.raw, "raw", false, "write binary records to stdout")

	if err := fs.Parse(args[2:]); err != nil {
		return nil, fmt.Errorf("subscribe: %w: %w", errInvalidOption, err)
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("subscribe: %w: %s", errTooManyArguments, fs.Arg(0))
	}

	return cmd, nil
}

func (cmd *subscribeCommand) Execute(ctx context.Context, env Env) (int, error) {
	return exitcode.General, fmt.Errorf("subscribe: %w", errors.ErrUnsupported)
}
