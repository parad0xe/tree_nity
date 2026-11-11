package commands

import (
	"context"
	"fmt"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
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

// Execute asks the server to create the topic.
func (cmd *createCommand) Execute(ctx context.Context, env Env) (int, error) {
	data, err := env.createData(
		protocol.CreateTopic,
		protocol.NewCreateTopicPayload(string(cmd.topic)),
	)
	if err != nil {
		return exitcode.General, fmt.Errorf("create: %w", err)
	}

	if code, err := env.send(ctx, data); err != nil {
		return code, fmt.Errorf("create: %w", err)
	}

	_, code, err := env.recv[*protocol.CreateTopicReplyPayload](ctx, protocol.CreateTopicReply)
	if err != nil {
		return code, fmt.Errorf("create: %w", err)
	}

	if err := env.println("topic created"); err != nil {
		return exitcode.General, fmt.Errorf("create: %w", err)
	}

	return exitcode.Success, nil
}
