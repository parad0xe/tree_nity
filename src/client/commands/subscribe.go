package commands

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
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

// Execute prints each message delivered from the topic.
func (cmd *subscribeCommand) Execute(ctx context.Context, env Env) (int, error) {
	subscriptionData, err := env.createData(
		protocol.TopicSubscription,
		protocol.NewTopicSubscriptionPayload(
			string(cmd.subscriber),
			string(cmd.topic),
			cmd.prefix,
			cmd.offset,
		),
	)
	if err != nil {
		return exitcode.General, fmt.Errorf("subscribe: %w", err)
	}

	if code, err := env.send(ctx, subscriptionData); err != nil {
		return code, fmt.Errorf("subscribe: %w", err)
	}

	_, code, err := env.recv[*protocol.TopicSubscriptionReplyPayload](ctx, protocol.TopicSubscriptionReply)
	if err != nil {
		return code, fmt.Errorf("subscribe: %w", err)
	}

	if err := env.println(fmt.Sprintf("subscribed to %s", cmd.topic)); err != nil {
		if _, disconnectErr := cmd.disconnect(ctx, env); disconnectErr != nil {
			err = errors.Join(err, disconnectErr)
		}
		return exitcode.General, fmt.Errorf("subscribe: %w", err)
	}

	recvCtx, recvCancel := context.WithCancel(ctx)
	defer recvCancel()
	go func() {
		select {
		case <-env.Shutdown.Requested():
		case <-recvCtx.Done():
		}
		recvCancel()
	}()

	code, err = cmd.consume(ctx, recvCtx, env)
	// A stopping server ends the subscription itself, so no disconnect is sent.
	if errors.Is(err, errServerShuttingDown) {
		return exitcode.Success, nil
	}
	if err != nil {
		err = fmt.Errorf("subscribe: %w", err)
	}

	if ctx.Err() == nil && code != exitcode.IPC {
		if disconnectCode, disconnectErr := cmd.disconnect(ctx, env); disconnectErr != nil {
			disconnectErr = fmt.Errorf("subscribe: %w", disconnectErr)
			if err == nil {
				return disconnectCode, disconnectErr
			}
			err = errors.Join(err, disconnectErr)
		}
	}

	return code, err
}

// consume prints and acknowledges the delivered messages.
func (cmd *subscribeCommand) consume(ctx context.Context, recvCtx context.Context, env Env) (int, error) {
	for {
		if env.Shutdown.IsRequested() {
			return exitcode.Success, nil
		}

		waitCtx, waitCancel := context.WithTimeout(recvCtx, env.RecvTimeout)
		data, code, err := env.recvData(waitCtx)
		waitCancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				continue
			}
			if env.Shutdown.IsRequested() {
				return exitcode.Success, nil
			}
			return code, err
		}

		switch data.Opcode() {
		case protocol.MessageDelivery:
			if code, err := cmd.deliver(ctx, env, data); err != nil {
				return code, err
			}
		case protocol.ServerConsumerDisconnect:
			return exitcode.IPC, errSubscriptionEnded
		default:
			if code, err := replyError(data); err != nil {
				return code, err
			}
			return exitcode.General, fmt.Errorf("recv: %w: %d", errUnexpectedOpcode, data.Opcode())
		}
	}
}

// deliver prints and acknowledges a delivered message.
func (cmd *subscribeCommand) deliver(ctx context.Context, env Env, data protocol.Data) (int, error) {
	messagePayload, ok := data.Payload().(*protocol.MessageDeliveryPayload)
	if !ok {
		return exitcode.General, fmt.Errorf("recv: %w", errUnexpectedPayload)
	}

	message := messagePayload.Message()

	if err := cmd.printMessage(env, message); err != nil {
		return exitcode.General, err
	}

	ackData, err := env.createData(
		protocol.Acknowledgement,
		protocol.NewAcknowledgementPayload(
			string(cmd.subscriber),
			message.Offset()+1,
		),
	)
	if err != nil {
		return exitcode.General, err
	}
	return env.send(ctx, ackData)
}

// disconnect ends the subscription on the server.
func (cmd *subscribeCommand) disconnect(ctx context.Context, env Env) (int, error) {
	disconnectPayload, err := env.createData(
		protocol.ConsumerDisconnect,
		protocol.NewConsumerDisconnectPayload(string(cmd.subscriber)),
	)
	if err != nil {
		return exitcode.General, err
	}

	if code, err := env.send(ctx, disconnectPayload); err != nil {
		return code, err
	}

	return exitcode.Success, nil
}

// printMessage writes a delivered message to the command output.
func (cmd *subscribeCommand) printMessage(env Env, message protocol.Message) error {
	if cmd.raw {
		buffer := make([]byte, 0)
		buffer = binary.LittleEndian.AppendUint32(buffer, message.Offset())
		buffer = binary.LittleEndian.AppendUint32(buffer, uint32(len(message.Key())))
		buffer = append(buffer, message.Key()...)
		buffer = binary.LittleEndian.AppendUint32(buffer, uint32(len(message.Value())))
		buffer = append(buffer, message.Value()...)

		if err := env.print(buffer); err != nil {
			return err
		}

		return nil
	}

	output := fmt.Sprintf("%s:%s", message.Key(), message.Value())

	if err := env.println(output); err != nil {
		return err
	}

	return nil
}
