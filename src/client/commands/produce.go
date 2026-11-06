package commands

import (
	"context"
	"errors"
	"fmt"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/id"
)

type produceCommand struct {
	topic id.ID
	raw   bool
}

// ParseProduce parses the arguments of the produce command.
func ParseProduce(args []string) (Command, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("produce: %w: <topic_name>", errMissingArgument)
	}

	topicID := id.ID(args[0])
	if err := topicID.Validate(); err != nil {
		return nil, fmt.Errorf("produce: topic %q: %w", topicID, err)
	}

	cmd := &produceCommand{topic: topicID}

	fs := newFlagSet("produce")
	fs.BoolVar(&cmd.raw, "raw", false, "read binary records from stdin")

	if err := fs.Parse(args[1:]); err != nil {
		return nil, fmt.Errorf("produce: %w: %w", errInvalidOption, err)
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("produce: %w: %s", errTooManyArguments, fs.Arg(0))
	}

	return cmd, nil
}

func (cmd *produceCommand) Execute(ctx context.Context, env Env) (int, error) {
	// TODO: send packet `check if topic exists` (exitcode 2 if not exists)
	// TODO: read message from stdin (using raw option) (in goroutine)
	// TODO: wait for goroutine response or ctx.Done
	// TODO: send packet `topic message` on Outbound
	return exitcode.General, fmt.Errorf("produce: %w", errors.ErrUnsupported)
}
