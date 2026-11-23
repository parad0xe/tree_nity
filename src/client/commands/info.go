package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
)

type infoCommand struct {
	subscriber id.ID
}

// consumerInfoOutput is the JSON written by the info command.
type consumerInfoOutput struct {
	Client string `json:"client"`
	Topic  string `json:"topic"`
	Offset uint32 `json:"offset"`
	Prefix string `json:"prefix"`
	IPC    string `json:"ipc"`
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

// Execute prints the saved metadata of the subscriber.
func (cmd *infoCommand) Execute(ctx context.Context, env Env) (int, error) {
	data, err := env.createData(
		protocol.ConsumerInfo,
		protocol.NewConsumerInfoPayload(string(cmd.subscriber)),
	)
	if err != nil {
		return exitcode.General, fmt.Errorf("info: %w", err)
	}

	if code, err := env.send(ctx, data); err != nil {
		return code, fmt.Errorf("info: %w", err)
	}

	payload, code, err := env.recv[*protocol.ConsumerInfoReplyPayload](ctx, protocol.ConsumerInfoReply)
	if err != nil {
		return code, fmt.Errorf("info: %w", err)
	}

	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(consumerInfoOutput{
		Client: payload.Client(),
		Topic:  payload.Topic(),
		Offset: payload.Offset(),
		Prefix: payload.Prefix(),
		IPC:    payload.IPC(),
	}); err != nil {
		return exitcode.General, fmt.Errorf("info: %w", err)
	}
	if err := env.print(output.Bytes()); err != nil {
		return exitcode.General, fmt.Errorf("info: %w", err)
	}

	return exitcode.Success, nil
}
