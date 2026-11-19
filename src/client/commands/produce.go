package commands

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
)

type produceCommand struct {
	topic id.ID
	raw   bool
}

// scanResult holds one message read from stdin, or the read error.
type scanResult struct {
	data []byte
	err  error
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

// Execute sends each message read from stdin to the topic.
func (cmd *produceCommand) Execute(ctx context.Context, env Env) (int, error) {
	checkTopicData, err := env.createData(
		protocol.CheckTopic,
		protocol.NewCheckTopicPayload(string(cmd.topic)),
	)
	if err != nil {
		return exitcode.General, fmt.Errorf("produce: %w", err)
	}

	if code, err := env.send(ctx, checkTopicData); err != nil {
		return code, fmt.Errorf("produce: %w", err)
	}

	_, code, err := env.recv[*protocol.CheckTopicReplyPayload](ctx, protocol.CheckTopicReply)
	if err != nil {
		return code, fmt.Errorf("produce: %w", err)
	}

	scanCtx, scanCancel := context.WithCancel(ctx)
	defer func() {
		scanCancel()
		_ = env.Stdin.Close()
	}()

	scanCh := make(chan scanResult)
	go cmd.scanStdin(scanCtx, env.Stdin, scanCh)

	for {
		select {
		case scan, open := <-scanCh:
			if !open {
				return exitcode.Success, nil
			}
			if scan.err != nil {
				return exitcode.General, fmt.Errorf("produce: %w", scan.err)
			}

			key, value, err := cmd.parseMessage(scan.data)
			if err != nil {
				return exitcode.General, fmt.Errorf("produce: %w", err)
			}

			produceMessageData, err := env.createData(
				protocol.ProduceMessage,
				protocol.NewProduceMessagePayload(string(cmd.topic), protocol.NewMessage(key, value)),
			)
			if err != nil {
				return exitcode.General, fmt.Errorf("produce: %w", err)
			}

			if code, err := env.send(ctx, produceMessageData); err != nil {
				return code, fmt.Errorf("produce: %w", err)
			}

			_, code, err = env.recv[*protocol.ProduceMessageReplyPayload](ctx, protocol.ProduceMessageReply)
			if err != nil {
				return code, fmt.Errorf("produce: %w", err)
			}

		case <-env.Shutdown.Requested():
			return exitcode.Success, nil

		case <-ctx.Done():
			return exitcode.Success, nil
		}
	}
}

// scanStdin reads the messages of stdin into scanCh.
func (cmd *produceCommand) scanStdin(ctx context.Context, stdin io.ReadCloser, scanCh chan<- scanResult) {
	defer close(scanCh)
	scanner := bufio.NewScanner(stdin)
	scanner.Split(cmd.splitMessage)

	for scanner.Scan() {
		data := append([]byte(nil), scanner.Bytes()...)

		select {
		case scanCh <- scanResult{data: data, err: nil}:
		case <-ctx.Done():
			return
		}
	}

	if err := scanner.Err(); err != nil {
		select {
		case scanCh <- scanResult{data: nil, err: err}:
			return
		case <-ctx.Done():
		}
	}
}

// readRawField returns the length-prefixed field at the start of data.
func readRawField(data []byte, usedSize int) ([]byte, error) {
	if len(data) < 4 {
		return nil, errIncompleteRecord
	}

	size := int(binary.LittleEndian.Uint32(data[:4]))

	if usedSize+size > protocol.MessageSize {
		return nil, protocol.ErrMessageTooLarge
	}

	data = data[4:]
	if len(data) < size {
		return nil, errIncompleteRecord
	}

	return data[:size], nil
}

// splitMessage is the split function of the stdin scanner.
func (cmd *produceCommand) splitMessage(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if len(data) == 0 {
		return 0, nil, nil
	}

	if cmd.raw {
		key, err := readRawField(data, 0)
		if err != nil {
			if !atEOF && errors.Is(err, errIncompleteRecord) {
				return 0, nil, nil
			}
			return 0, nil, err
		}
		if len(key) == 0 {
			return 0, nil, errEmptyKey
		}

		value, err := readRawField(data[4+len(key):], len(key))
		if err != nil {
			if !atEOF && errors.Is(err, errIncompleteRecord) {
				return 0, nil, nil
			}
			return 0, nil, err
		}
		if len(value) == 0 {
			return 0, nil, errEmptyValue
		}

		recordSize := 4 + len(key) + 4 + len(value)
		return recordSize, data[:recordSize], nil
	}

	if index := bytes.IndexByte(data, '\n'); index >= 0 {
		message := bytes.TrimSuffix(data[:index], []byte{'\r'})
		return index + 1, message, nil
	}

	if atEOF {
		return len(data), data, nil
	}

	return 0, nil, nil
}

// parseMessage returns the key and value of a message.
func (cmd *produceCommand) parseMessage(data []byte) ([]byte, []byte, error) {
	if cmd.raw {
		key, err := readRawField(data, 0)
		if err != nil {
			return nil, nil, err
		}

		value, err := readRawField(data[4+len(key):], len(key))
		if err != nil {
			return nil, nil, err
		}

		return key, value, nil
	}

	values := strings.SplitN(string(data), ":", 2)
	if len(values) != 2 {
		return nil, nil, errInvalidLine
	}
	if len(values[0]) == 0 {
		return nil, nil, errEmptyKey
	}
	if len(values[1]) == 0 {
		return nil, nil, errEmptyValue
	}
	return []byte(values[0]), []byte(values[1]), nil
}
