package commands

import (
	"context"
	"errors"
	"fmt"

	"tree_nity/src/exitcode"
)

type listCommand struct{}

// ParseList parses the arguments of the list command.
func ParseList(args []string) (Command, error) {
	if len(args) >= 1 {
		return nil, fmt.Errorf("list: %w: %s", errTooManyArguments, args[0])
	}

	return &listCommand{}, nil
}

func (cmd *listCommand) Execute(ctx context.Context, env Env) (int, error) {
	return exitcode.General, fmt.Errorf("list: %w", errors.ErrUnsupported)
}
