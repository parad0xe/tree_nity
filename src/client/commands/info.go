package commands

import (
	"context"
	"errors"
	"fmt"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/id"
)

type infoCommand struct {
	subscriber id.ID
}

// ParseInfo parses the arguments of the info command.
func ParseInfo(args []string) (Command, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("info: %w: <subscriber_name>", errMissingArgument)
	}

	subscriber := id.ID(args[0])
	if err := subscriber.Validate(); err != nil {
		return nil, fmt.Errorf("info: subscriber %q: %w", subscriber, err)
	}

	if len(args) > 1 {
		return nil, fmt.Errorf("info: %w: %s", errTooManyArguments, args[1])
	}

	return &infoCommand{subscriber: subscriber}, nil
}

func (cmd *infoCommand) Execute(ctx context.Context, env Env) (int, error) {
	return exitcode.General, fmt.Errorf("info: %w", errors.ErrUnsupported)
}
