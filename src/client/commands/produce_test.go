package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
)

// testMessage is a key and value that a produce request carries.
type testMessage struct {
	key   string
	value string
}

// executeResult is what Execute returned.
type executeResult struct {
	code int
	err  error
}

// newProduceEnv returns an Env reading stdin, whose inbound already holds the
// replies in order, with the queue of the server.
func newProduceEnv(t *testing.T, stdin io.ReadCloser, reply protocol.Data, nextReplies ...protocol.Data) (Env, *ipc.Inbound) {
	t.Helper()
	env, server, _ := newCommandEnv(t, reply)
	client := newTestOutbound(t, env.Inbound.IPCPath())
	for _, next := range nextReplies {
		if err := client.Send(context.Background(), next); err != nil {
			t.Fatalf("Send(%d) error = %v, want nil", next.Opcode(), err)
		}
	}
	env.Stdin = stdin
	return env, server
}

// newCheckTopicReply returns the reply of the server to an existing topic.
func newCheckTopicReply(t *testing.T) protocol.Data {
	t.Helper()
	return newServerData(t, protocol.CheckTopicReply, protocol.NewCheckTopicReplyPayload())
}

// newProduceMessageReplies returns count replies of the server to produced messages.
func newProduceMessageReplies(t *testing.T, count int) []protocol.Data {
	t.Helper()
	replies := make([]protocol.Data, count)
	for i := range replies {
		replies[i] = newServerData(t, protocol.ProduceMessageReply, protocol.NewProduceMessageReplyPayload())
	}
	return replies
}

// newProduceMessageData returns the request that produces key and value to topic.
func newProduceMessageData(t *testing.T, env Env, topic id.ID, key, value []byte) protocol.Data {
	t.Helper()
	data, err := protocol.NewData(
		protocol.ProduceMessage,
		protocol.NewClientMetadata(env.Inbound.IPCPath()),
		protocol.NewProduceMessagePayload(string(topic), protocol.NewMessage(key, value)),
	)
	if err != nil {
		t.Fatalf("NewData(%d) error = %v, want nil", protocol.ProduceMessage, err)
	}
	return data
}

// rawRecord returns key and value in the binary format of stdin.
func rawRecord(key, value []byte) []byte {
	record := binary.LittleEndian.AppendUint32(nil, uint32(len(key)))
	record = append(record, key...)
	record = binary.LittleEndian.AppendUint32(record, uint32(len(value)))
	return append(record, value...)
}

// requestedShutdown returns a shutdown that is already requested.
func requestedShutdown() lifecycle.GracefulShutdown {
	requested := make(chan struct{})
	close(requested)
	return lifecycle.NewGracefulShutdown(requested)
}

// executeWithTimeout runs cmd, and fails the test if it does not return within testTimeout.
func executeWithTimeout(t *testing.T, cmd Command, env Env) (int, error) {
	t.Helper()
	result := make(chan executeResult, 1)
	go func() {
		code, err := cmd.Execute(context.Background(), env)
		result <- executeResult{code: code, err: err}
	}()
	select {
	case res := <-result:
		return res.code, res.err
	case <-time.After(testTimeout):
		t.Fatalf("Execute() did not return within %v", testTimeout)
		return 0, nil
	}
}

func TestParseProduceReadsTopic(t *testing.T) {
	args := []string{"orders"}

	command, err := ParseProduce(args)

	if err != nil {
		t.Fatalf("ParseProduce(%q) error = %v, want nil", args, err)
	}
	p, ok := command.(*produceCommand)
	if !ok {
		t.Fatalf("ParseProduce(%q) = %T, want *produceCommand", args, command)
	}
	if want := id.ID(args[0]); p.topic != want {
		t.Errorf("ParseProduce(%q) topic = %q, want %q", args, p.topic, want)
	}
	if p.raw {
		t.Errorf("ParseProduce(%q) raw = %v, want %v", args, p.raw, false)
	}
}

func TestParseProduceReadsRawOption(t *testing.T) {
	args := []string{"orders", "--raw"}

	command, err := ParseProduce(args)

	if err != nil {
		t.Fatalf("ParseProduce(%q) error = %v, want nil", args, err)
	}
	p, ok := command.(*produceCommand)
	if !ok {
		t.Fatalf("ParseProduce(%q) = %T, want *produceCommand", args, command)
	}
	if !p.raw {
		t.Errorf("ParseProduce(%q) raw = %v, want %v", args, p.raw, true)
	}
}

func TestParseProduceRejectsMissingTopic(t *testing.T) {
	args := []string{}

	if _, err := ParseProduce(args); !errors.Is(err, errMissingArgument) {
		t.Errorf("ParseProduce(%q) error = %v, want %v", args, err, errMissingArgument)
	}
}

func TestParseProduceRejectsInvalidTopic(t *testing.T) {
	args := []string{"bad topic"}

	if _, err := ParseProduce(args); !errors.Is(err, id.ErrInvalidID) {
		t.Errorf("ParseProduce(%q) error = %v, want %v", args, err, id.ErrInvalidID)
	}
}

func TestParseProduceRejectsUnknownOption(t *testing.T) {
	args := []string{"orders", "--verbose"}

	if _, err := ParseProduce(args); !errors.Is(err, errInvalidOption) {
		t.Errorf("ParseProduce(%q) error = %v, want %v", args, err, errInvalidOption)
	}
}

func TestParseProduceRejectsExtraArgument(t *testing.T) {
	args := []string{"orders", "extra"}

	if _, err := ParseProduce(args); !errors.Is(err, errTooManyArguments) {
		t.Errorf("ParseProduce(%q) error = %v, want %v", args, err, errTooManyArguments)
	}
}

func TestProduceExecuteSendsCheckTopicRequest(t *testing.T) {
	cmd := &produceCommand{topic: "orders"}
	env, server := newProduceEnv(t, io.NopCloser(strings.NewReader("")), newCheckTopicReply(t))

	if code, err := cmd.Execute(context.Background(), env); err != nil {
		t.Fatalf("Execute() for topic %q = %d, %v, want %d, nil", cmd.topic, code, err, exitcode.Success)
	}

	want, err := protocol.NewData(
		protocol.CheckTopic,
		protocol.NewClientMetadata(env.Inbound.IPCPath()),
		protocol.NewCheckTopicPayload(string(cmd.topic)),
	)
	if err != nil {
		t.Fatalf("NewData(%d) error = %v, want nil", protocol.CheckTopic, err)
	}
	if got := recvTestData(t, server); !reflect.DeepEqual(got, want) {
		t.Errorf("first request of Execute() for topic %q = %+v, want %+v", cmd.topic, got, want)
	}
}

func TestProduceExecuteReturnsTopicCodeWhenTopicIsMissing(t *testing.T) {
	cmd := &produceCommand{topic: "orders"}
	reply := protocol.NewErrorReplyPayload(protocol.TopicNotFound, "topic not found")
	env, _ := newProduceEnv(t, io.NopCloser(strings.NewReader("")), newServerData(t, protocol.ErrorReply, reply))

	code, err := cmd.Execute(context.Background(), env)

	if code != exitcode.Topic || !errors.Is(err, errRequestRejected) {
		t.Errorf("Execute() for missing topic %q = %d, %v, want %d, %v", cmd.topic, code, err, exitcode.Topic, errRequestRejected)
	}
}

func TestProduceExecuteReturnsSuccessAtEndOfInput(t *testing.T) {
	cmd := &produceCommand{topic: "orders"}
	env, _ := newProduceEnv(t, io.NopCloser(strings.NewReader("")), newCheckTopicReply(t))

	code, err := cmd.Execute(context.Background(), env)

	if err != nil || code != exitcode.Success {
		t.Errorf("Execute() for topic %q with an empty stdin = %d, %v, want %d, nil", cmd.topic, code, err, exitcode.Success)
	}
}

func TestProduceExecuteSendsTextMessages(t *testing.T) {
	tests := []struct {
		name  string
		stdin string
		want  []testMessage
	}{
		{name: "line feeds", stdin: "a:1\nb:2\n", want: []testMessage{{key: "a", value: "1"}, {key: "b", value: "2"}}},
		{name: "carriage returns", stdin: "a:1\r\nb:2\r\n", want: []testMessage{{key: "a", value: "1"}, {key: "b", value: "2"}}},
		{name: "no final line feed", stdin: "a:1\nb:2", want: []testMessage{{key: "a", value: "1"}, {key: "b", value: "2"}}},
		{name: "final carriage return", stdin: "a:1\nb:2\r", want: []testMessage{{key: "a", value: "1"}, {key: "b", value: "2"}}},
		{name: "separator in value", stdin: "a:1:2\n", want: []testMessage{{key: "a", value: "1:2"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &produceCommand{topic: "orders"}
			env, server := newProduceEnv(t, io.NopCloser(strings.NewReader(tt.stdin)), newCheckTopicReply(t), newProduceMessageReplies(t, len(tt.want))...)

			if code, err := cmd.Execute(context.Background(), env); err != nil || code != exitcode.Success {
				t.Fatalf("Execute() with stdin %q = %d, %v, want %d, nil", tt.stdin, code, err, exitcode.Success)
			}

			recvTestData(t, server)
			for i, message := range tt.want {
				want := newProduceMessageData(t, env, cmd.topic, []byte(message.key), []byte(message.value))
				if got := recvTestData(t, server); !reflect.DeepEqual(got, want) {
					t.Errorf("message %d of Execute() with stdin %q = %+v, want %+v", i, tt.stdin, got, want)
				}
			}
		})
	}
}

func TestProduceExecuteSendsRawRecords(t *testing.T) {
	cmd := &produceCommand{topic: "orders", raw: true}
	want := []testMessage{{key: "0123456789", value: "v\r"}, {key: "k", value: "\n"}}
	var stdin []byte
	for _, message := range want {
		stdin = append(stdin, rawRecord([]byte(message.key), []byte(message.value))...)
	}
	env, server := newProduceEnv(t, io.NopCloser(bytes.NewReader(stdin)), newCheckTopicReply(t), newProduceMessageReplies(t, len(want))...)

	if code, err := cmd.Execute(context.Background(), env); err != nil || code != exitcode.Success {
		t.Fatalf("Execute() with raw stdin %q = %d, %v, want %d, nil", stdin, code, err, exitcode.Success)
	}

	recvTestData(t, server)
	for i, message := range want {
		data := newProduceMessageData(t, env, cmd.topic, []byte(message.key), []byte(message.value))
		if got := recvTestData(t, server); !reflect.DeepEqual(got, data) {
			t.Errorf("message %d of Execute() with raw stdin %q = %+v, want %+v", i, stdin, got, data)
		}
	}
}

func TestProduceExecuteRejectsInvalidText(t *testing.T) {
	tests := []struct {
		name    string
		stdin   string
		wantErr error
	}{
		{name: "no separator", stdin: "abc\n", wantErr: errInvalidLine},
		{name: "empty key", stdin: ":v\n", wantErr: errEmptyKey},
		{name: "empty value", stdin: "k:\n", wantErr: errEmptyValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &produceCommand{topic: "orders"}
			env, _ := newProduceEnv(t, io.NopCloser(strings.NewReader(tt.stdin)), newCheckTopicReply(t))

			code, err := cmd.Execute(context.Background(), env)

			if code != exitcode.General || !errors.Is(err, tt.wantErr) {
				t.Errorf("Execute() with stdin %q = %d, %v, want %d, %v", tt.stdin, code, err, exitcode.General, tt.wantErr)
			}
		})
	}
}

func TestProduceExecuteRejectsInvalidRecords(t *testing.T) {
	record := rawRecord([]byte("k"), []byte("v"))
	tests := []struct {
		name    string
		stdin   []byte
		wantErr error
	}{
		{name: "incomplete record", stdin: record[:len(record)-1], wantErr: errIncompleteRecord},
		{name: "empty key", stdin: rawRecord(nil, []byte("v")), wantErr: errEmptyKey},
		{name: "empty value", stdin: rawRecord([]byte("k"), nil), wantErr: errEmptyValue},
		{name: "too large", stdin: rawRecord([]byte("k"), make([]byte, protocol.MessageSize)), wantErr: protocol.ErrMessageTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &produceCommand{topic: "orders", raw: true}
			env, _ := newProduceEnv(t, io.NopCloser(bytes.NewReader(tt.stdin)), newCheckTopicReply(t))

			code, err := cmd.Execute(context.Background(), env)

			if code != exitcode.General || !errors.Is(err, tt.wantErr) {
				t.Errorf("Execute() with raw stdin %q = %d, %v, want %d, %v", tt.stdin, code, err, exitcode.General, tt.wantErr)
			}
		})
	}
}

func TestProduceExecuteRejectsOversizedRecordWhileStdinIsOpen(t *testing.T) {
	cmd := &produceCommand{topic: "orders", raw: true}
	stdin, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	env, _ := newProduceEnv(t, stdin, newCheckTopicReply(t))
	keySize := binary.LittleEndian.AppendUint32(nil, uint32(protocol.MessageSize+1))
	go func() { _, _ = writer.Write(keySize) }()

	code, err := executeWithTimeout(t, cmd, env)

	if code != exitcode.General || !errors.Is(err, protocol.ErrMessageTooLarge) {
		t.Errorf("Execute() with key size %d and an open stdin = %d, %v, want %d, %v", protocol.MessageSize+1, code, err, exitcode.General, protocol.ErrMessageTooLarge)
	}
}

func TestProduceExecuteReturnsMessageRejection(t *testing.T) {
	cmd := &produceCommand{topic: "orders"}
	reply := protocol.NewErrorReplyPayload(protocol.TopicNotFound, "topic not found")
	env, _ := newProduceEnv(t, io.NopCloser(strings.NewReader("a:1\n")), newCheckTopicReply(t), newServerData(t, protocol.ErrorReply, reply))

	code, err := cmd.Execute(context.Background(), env)

	if code != exitcode.Topic || !errors.Is(err, errRequestRejected) {
		t.Errorf("Execute() with a rejected message = %d, %v, want %d, %v", code, err, exitcode.Topic, errRequestRejected)
	}
}

func TestProduceExecuteStopsOnShutdownRequest(t *testing.T) {
	cmd := &produceCommand{topic: "orders"}
	stdin, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	env, _ := newProduceEnv(t, stdin, newCheckTopicReply(t))
	env.Shutdown = requestedShutdown()

	code, err := executeWithTimeout(t, cmd, env)

	if err != nil || code != exitcode.Success {
		t.Errorf("Execute() after a shutdown request with an open stdin = %d, %v, want %d, nil", code, err, exitcode.Success)
	}
}

func TestProduceExecuteClosesStdin(t *testing.T) {
	cmd := &produceCommand{topic: "orders"}
	stdin, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	env, _ := newProduceEnv(t, stdin, newCheckTopicReply(t))
	env.Shutdown = requestedShutdown()

	if code, err := executeWithTimeout(t, cmd, env); err != nil {
		t.Fatalf("Execute() after a shutdown request = %d, %v, want %d, nil", code, err, exitcode.Success)
	}

	if _, err := writer.Write([]byte("a:1\n")); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("Write() to stdin after Execute() error = %v, want %v", err, io.ErrClosedPipe)
	}
}
