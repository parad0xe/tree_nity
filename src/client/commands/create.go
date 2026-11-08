package commands

import (
	"context"
	"errors"
	"fmt"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/id"
)

type createCommand struct {
	topic id.ID
}

// ParseCreate parses the arguments of the create command.
func ParseCreate(args []string) (Command, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("create: %w: <topic_name>", errMissingArgument)
	}

	topicID := id.ID(args[0])
	if err := topicID.Validate(); err != nil {
		return nil, fmt.Errorf("create: topic %q: %w", topicID, err)
	}

	if len(args) > 1 {
		return nil, fmt.Errorf("create: %w: %s", errTooManyArguments, args[1])
	}

	return &createCommand{topic: topicID}, nil
}

func (cmd *createCommand) Execute(ctx context.Context, env Env) (int, error) {
	return exitcode.General, fmt.Errorf("create: %w", errors.ErrUnsupported)
}
