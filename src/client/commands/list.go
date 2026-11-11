package commands

import (
	"context"
	"fmt"
	"strings"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/protocol"
)

type listCommand struct{}

// ParseList parses the arguments of the list command.
func ParseList(args []string) (Command, error) {
	if len(args) >= 1 {
		return nil, fmt.Errorf("list: %w: %s", errTooManyArguments, args[0])
	}

	return &listCommand{}, nil
}

// Execute prints the names of the server topics.
func (cmd *listCommand) Execute(ctx context.Context, env Env) (int, error) {
	data, err := env.createData(
		protocol.ListTopic,
		protocol.NewListTopicPayload(),
	)
	if err != nil {
		return exitcode.General, fmt.Errorf("list: %w", err)
	}

	if code, err := env.send(ctx, data); err != nil {
		return code, fmt.Errorf("list: %w", err)
	}

	payload, code, err := env.recv[*protocol.ListTopicReplyPayload](ctx, protocol.ListTopicReply)
	if err != nil {
		return code, fmt.Errorf("list: %w", err)
	}

	if err := env.println(strings.Join(payload.Topics(), ",")); err != nil {
		return exitcode.General, fmt.Errorf("list: %w", err)
	}

	return exitcode.Success, nil
}
