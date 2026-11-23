package commands

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
	"tree_nity/src/lifecycle"
)

// writerFailingAfter is a mock writer whose writes fail once allowed writes succeeded.
type writerFailingAfter struct {
	allowed int
}

func (w *writerFailingAfter) Write(p []byte) (int, error) {
	if w.allowed == 0 {
		return 0, errTestWrite
	}
	w.allowed--
	return len(p), nil
}

// parseSubscribeCommand returns the subscribe command parsed from args.
func parseSubscribeCommand(t *testing.T, args []string) *subscribeCommand {
	t.Helper()

	command, err := ParseSubscribe(args)
	if err != nil {
		t.Fatalf("ParseSubscribe(%q) error = %v, want nil", args, err)
	}
	cmd, ok := command.(*subscribeCommand)
	if !ok {
		t.Fatalf("ParseSubscribe(%q) = %T, want *subscribeCommand", args, command)
	}
	return cmd
}

// checkOffset reports an error unless the offset parsed from args equals want.
func checkOffset(t *testing.T, args []string, want uint32) {
	t.Helper()

	cmd := parseSubscribeCommand(t, args)
	if cmd.offset == nil {
		t.Fatalf("ParseSubscribe(%q) offset = nil, want %d", args, want)
	}
	if *cmd.offset != want {
		t.Errorf("ParseSubscribe(%q) offset = %d, want %d", args, *cmd.offset, want)
	}
}

// newSubscribeEnv returns an Env whose inbound already holds the replies in
// order, with the queue of the server and the standard output of the command.
func newSubscribeEnv(t *testing.T, reply protocol.Data, nextReplies ...protocol.Data) (Env, *ipc.Inbound, *bytes.Buffer) {
	t.Helper()
	env, server, stdout := newCommandEnv(t, reply)
	sendTestReplies(t, env, nextReplies...)
	return env, server, stdout
}

// newSubscriptionReply returns the reply of the server to an accepted subscription.
func newSubscriptionReply(t *testing.T) protocol.Data {
	t.Helper()
	return newServerData(t, protocol.TopicSubscriptionReply, protocol.NewTopicSubscriptionReplyPayload())
}

// newDeliveryData returns the delivery of key and value at offset.
func newDeliveryData(t *testing.T, offset uint32, key, value string) protocol.Data {
	t.Helper()
	payload := protocol.NewMessageDeliveryPayload(offset, protocol.NewMessage([]byte(key), []byte(value)))
	return newServerData(t, protocol.MessageDelivery, payload)
}

// newSubscriberData returns a request of the subscriber that env sends.
func newSubscriberData(t *testing.T, env Env, opcode protocol.Opcode, payload protocol.Payload) protocol.Data {
	t.Helper()
	data, err := protocol.NewData(opcode, protocol.NewClientMetadata(env.Inbound.IPCPath()), payload)
	if err != nil {
		t.Fatalf("NewData(%d) error = %v, want nil", opcode, err)
	}
	return data
}

// newShutdown returns a shutdown that is requested once request is called.
func newShutdown() (lifecycle.GracefulShutdown, func()) {
	requested := make(chan struct{})
	return lifecycle.NewGracefulShutdown(requested), func() { close(requested) }
}

// assertNoTestData fails the test if the queue receives a frame within shortRecvTimeout.
func assertNoTestData(t *testing.T, in *ipc.Inbound) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), shortRecvTimeout)
	defer cancel()
	if data, err := in.Recv(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Recv() = %+v, %v, want no frame and %v", data, err, context.DeadlineExceeded)
	}
}

func TestParseSubscribeReadsTopic(t *testing.T) {
	args := []string{"orders", "client0"}

	cmd := parseSubscribeCommand(t, args)

	if want := id.ID(args[0]); cmd.topic != want {
		t.Errorf("ParseSubscribe(%q) topic = %q, want %q", args, cmd.topic, want)
	}
}

func TestParseSubscribeReadsSubscriber(t *testing.T) {
	args := []string{"orders", "client0"}

	cmd := parseSubscribeCommand(t, args)

	if want := id.ID(args[1]); cmd.subscriber != want {
		t.Errorf("ParseSubscribe(%q) subscriber = %q, want %q", args, cmd.subscriber, want)
	}
}

func TestParseSubscribeDefaultsToNoOffset(t *testing.T) {
	args := []string{"orders", "client0"}

	cmd := parseSubscribeCommand(t, args)

	if cmd.offset != nil {
		t.Errorf("ParseSubscribe(%q) offset = %d, want nil", args, *cmd.offset)
	}
}

func TestParseSubscribeDefaultsToEmptyPrefix(t *testing.T) {
	args := []string{"orders", "client0"}

	cmd := parseSubscribeCommand(t, args)

	if cmd.prefix != "" {
		t.Errorf("ParseSubscribe(%q) prefix = %q, want empty", args, cmd.prefix)
	}
}

func TestParseSubscribeDefaultsToNoRaw(t *testing.T) {
	args := []string{"orders", "client0"}

	cmd := parseSubscribeCommand(t, args)

	if cmd.raw {
		t.Errorf("ParseSubscribe(%q) raw = %v, want %v", args, cmd.raw, false)
	}
}

func TestParseSubscribeReadsPrefixOption(t *testing.T) {
	args := []string{"orders", "client0", "--prefix", "user.create"}

	cmd := parseSubscribeCommand(t, args)

	if want := args[3]; cmd.prefix != want {
		t.Errorf("ParseSubscribe(%q) prefix = %q, want %q", args, cmd.prefix, want)
	}
}

func TestParseSubscribeReadsRawOption(t *testing.T) {
	args := []string{"orders", "client0", "--raw"}

	cmd := parseSubscribeCommand(t, args)

	if !cmd.raw {
		t.Errorf("ParseSubscribe(%q) raw = %v, want %v", args, cmd.raw, true)
	}
}

func TestParseSubscribeReadsOffsetOption(t *testing.T) {
	checkOffset(t, []string{"orders", "client0", "--offset", "42"}, 42)
}

func TestParseSubscribeReadsZeroOffset(t *testing.T) {
	checkOffset(t, []string{"orders", "client0", "--offset", "0"}, 0)
}

func TestParseSubscribeReadsMaximumOffset(t *testing.T) {
	checkOffset(t, []string{"orders", "client0", "--offset", "4294967295"}, 4294967295)
}

func TestParseSubscribeReadsOffsetInBaseTen(t *testing.T) {
	checkOffset(t, []string{"orders", "client0", "--offset", "010"}, 10)
}

func TestParseSubscribeRejectsMissingTopic(t *testing.T) {
	args := []string{}

	if _, err := ParseSubscribe(args); !errors.Is(err, errMissingArgument) {
		t.Errorf("ParseSubscribe(%q) error = %v, want %v", args, err, errMissingArgument)
	}
}

func TestParseSubscribeRejectsInvalidTopic(t *testing.T) {
	args := []string{"bad topic", "client0"}

	if _, err := ParseSubscribe(args); !errors.Is(err, id.ErrInvalidID) {
		t.Errorf("ParseSubscribe(%q) error = %v, want %v", args, err, id.ErrInvalidID)
	}
}

func TestParseSubscribeRejectsMissingSubscriber(t *testing.T) {
	args := []string{"orders"}

	if _, err := ParseSubscribe(args); !errors.Is(err, errMissingArgument) {
		t.Errorf("ParseSubscribe(%q) error = %v, want %v", args, err, errMissingArgument)
	}
}

func TestParseSubscribeRejectsInvalidSubscriber(t *testing.T) {
	args := []string{"orders", "bad subscriber"}

	if _, err := ParseSubscribe(args); !errors.Is(err, id.ErrInvalidID) {
		t.Errorf("ParseSubscribe(%q) error = %v, want %v", args, err, id.ErrInvalidID)
	}
}

func TestParseSubscribeRejectsUnknownOption(t *testing.T) {
	args := []string{"orders", "client0", "--verbose"}

	if _, err := ParseSubscribe(args); !errors.Is(err, errInvalidOption) {
		t.Errorf("ParseSubscribe(%q) error = %v, want %v", args, err, errInvalidOption)
	}
}

func TestParseSubscribeRejectsNonNumericOffset(t *testing.T) {
	args := []string{"orders", "client0", "--offset", "abc"}

	if _, err := ParseSubscribe(args); !errors.Is(err, errInvalidOption) {
		t.Errorf("ParseSubscribe(%q) error = %v, want %v", args, err, errInvalidOption)
	}
}

func TestParseSubscribeRejectsNegativeOffset(t *testing.T) {
	args := []string{"orders", "client0", "--offset", "-1"}

	if _, err := ParseSubscribe(args); !errors.Is(err, errInvalidOption) {
		t.Errorf("ParseSubscribe(%q) error = %v, want %v", args, err, errInvalidOption)
	}
}

func TestParseSubscribeRejectsOffsetAboveMaximum(t *testing.T) {
	args := []string{"orders", "client0", "--offset", "4294967296"}

	if _, err := ParseSubscribe(args); !errors.Is(err, errInvalidOption) {
		t.Errorf("ParseSubscribe(%q) error = %v, want %v", args, err, errInvalidOption)
	}
}

func TestParseSubscribeRejectsExtraArgument(t *testing.T) {
	args := []string{"orders", "client0", "extra"}

	if _, err := ParseSubscribe(args); !errors.Is(err, errTooManyArguments) {
		t.Errorf("ParseSubscribe(%q) error = %v, want %v", args, err, errTooManyArguments)
	}
}

func TestSubscribeExecuteSendsSubscriptionRequest(t *testing.T) {
	offset := uint32(4)
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0", prefix: "user.", offset: &offset}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t))
	env.Shutdown = requestedShutdown()

	if code, err := cmd.Execute(context.Background(), env); err != nil {
		t.Fatalf("Execute() for topic %q = %d, %v, want %d, nil", cmd.topic, code, err, exitcode.Success)
	}

	payload := protocol.NewTopicSubscriptionPayload(string(cmd.subscriber), string(cmd.topic), cmd.prefix, cmd.offset)
	want := newSubscriberData(t, env, protocol.TopicSubscription, payload)
	if got := recvTestData(t, server); !reflect.DeepEqual(got, want) {
		t.Errorf("first request of Execute() for topic %q = %+v, want %+v", cmd.topic, got, want)
	}
}

func TestSubscribeExecutePrintsConfirmation(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, _, stdout := newSubscribeEnv(t, newSubscriptionReply(t))
	env.Shutdown = requestedShutdown()

	code, err := cmd.Execute(context.Background(), env)

	if err != nil || code != exitcode.Success {
		t.Fatalf("Execute() for topic %q = %d, %v, want %d, nil", cmd.topic, code, err, exitcode.Success)
	}
	if got, want := stdout.String(), fmt.Sprintf("subscribed to %s\n", cmd.topic); got != want {
		t.Errorf("Execute() for topic %q wrote %q, want %q", cmd.topic, got, want)
	}
}

func TestSubscribeExecuteReturnsTopicCodeWhenRejected(t *testing.T) {
	tests := []struct {
		name string
		kind protocol.ErrorKind
	}{
		{name: "topic not found", kind: protocol.TopicNotFound},
		{name: "client name taken", kind: protocol.ClientNameTaken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
			reply := protocol.NewErrorReplyPayload(tt.kind, tt.name)
			env, _, _ := newSubscribeEnv(t, newServerData(t, protocol.ErrorReply, reply))

			code, err := cmd.Execute(context.Background(), env)

			if code != exitcode.Topic || !errors.Is(err, errRequestRejected) {
				t.Errorf("Execute() with an error reply of kind %d = %d, %v, want %d, %v", tt.kind, code, err, exitcode.Topic, errRequestRejected)
			}
		})
	}
}

func TestSubscribeExecutePrintsTextMessages(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	messages := []testMessage{{key: "a", value: "1"}, {key: "b", value: "2"}}
	deliveries := make([]protocol.Data, len(messages))
	want := fmt.Sprintf("subscribed to %s\n", cmd.topic)
	for i, message := range messages {
		deliveries[i] = newDeliveryData(t, uint32(i), message.key, message.value)
		want += message.key + ":" + message.value + "\n"
	}
	env, server, stdout := newSubscribeEnv(t, newSubscriptionReply(t), deliveries...)
	shutdown, requestShutdown := newShutdown()
	env.Shutdown = shutdown

	result := startExecute(cmd, env)
	// Wait for the subscription request and one acknowledgement per message.
	for range len(messages) + 1 {
		recvTestData(t, server)
	}
	requestShutdown()

	if code, err := waitExecute(t, result); err != nil || code != exitcode.Success {
		t.Fatalf("Execute() after %d messages and a shutdown = %d, %v, want %d, nil", len(messages), code, err, exitcode.Success)
	}
	if got := stdout.String(); got != want {
		t.Errorf("Execute() wrote %q, want %q", got, want)
	}
}

func TestSubscribeExecutePrintsRawMessages(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0", raw: true}
	messages := []testMessage{{key: "0123456789", value: "v\r"}, {key: "k", value: "\n"}}
	deliveries := make([]protocol.Data, len(messages))
	want := []byte(fmt.Sprintf("subscribed to %s\n", cmd.topic))
	for i, message := range messages {
		deliveries[i] = newDeliveryData(t, uint32(i), message.key, message.value)
		want = binary.LittleEndian.AppendUint32(want, uint32(i))
		want = append(want, rawRecord([]byte(message.key), []byte(message.value))...)
	}
	env, server, stdout := newSubscribeEnv(t, newSubscriptionReply(t), deliveries...)
	shutdown, requestShutdown := newShutdown()
	env.Shutdown = shutdown

	result := startExecute(cmd, env)
	// Wait for the subscription request and one acknowledgement per message.
	for range len(messages) + 1 {
		recvTestData(t, server)
	}
	requestShutdown()

	if code, err := waitExecute(t, result); err != nil || code != exitcode.Success {
		t.Fatalf("Execute() after %d raw messages and a shutdown = %d, %v, want %d, nil", len(messages), code, err, exitcode.Success)
	}
	if got := stdout.Bytes(); !bytes.Equal(got, want) {
		t.Errorf("Execute() wrote %q, want %q", got, want)
	}
}

func TestSubscribeExecuteAcknowledgesNextOffset(t *testing.T) {
	const offset = 4
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t), newDeliveryData(t, offset, "a", "1"))
	shutdown, requestShutdown := newShutdown()
	env.Shutdown = shutdown

	result := startExecute(cmd, env)
	recvTestData(t, server)
	got := recvTestData(t, server)
	requestShutdown()

	if code, err := waitExecute(t, result); err != nil || code != exitcode.Success {
		t.Fatalf("Execute() after a delivery at offset %d and a shutdown = %d, %v, want %d, nil", offset, code, err, exitcode.Success)
	}
	want := newSubscriberData(t, env, protocol.Acknowledgement, protocol.NewAcknowledgementPayload(string(cmd.subscriber), offset+1))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("request of Execute() after a delivery at offset %d = %+v, want %+v", offset, got, want)
	}
}

func TestSubscribeExecuteSendsDisconnectOnShutdown(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t))
	shutdown, requestShutdown := newShutdown()
	env.Shutdown = shutdown

	result := startExecute(cmd, env)
	recvTestData(t, server)
	requestShutdown()

	if code, err := waitExecute(t, result); err != nil || code != exitcode.Success {
		t.Fatalf("Execute() after a shutdown = %d, %v, want %d, nil", code, err, exitcode.Success)
	}
	want := newSubscriberData(t, env, protocol.ConsumerDisconnect, protocol.NewConsumerDisconnectPayload(string(cmd.subscriber)))
	if got := recvTestData(t, server); !reflect.DeepEqual(got, want) {
		t.Errorf("request of Execute() after a shutdown = %+v, want %+v", got, want)
	}
}

func TestSubscribeExecuteStopsWaitingOnShutdown(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t))
	shutdown, requestShutdown := newShutdown()
	env.Shutdown = shutdown

	result := startExecute(cmd, env)
	recvTestData(t, server)
	start := time.Now()
	requestShutdown()

	if code, err := waitExecute(t, result); err != nil || code != exitcode.Success {
		t.Fatalf("Execute() after a shutdown = %d, %v, want %d, nil", code, err, exitcode.Success)
	}
	if elapsed := time.Since(start); elapsed >= env.RecvTimeout/2 {
		t.Errorf("Execute() returned %v after a shutdown, want less than %v", elapsed, env.RecvTimeout/2)
	}
}

func TestSubscribeExecuteDoesNotAcknowledgeUnprintedMessage(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t), newDeliveryData(t, 0, "a", "1"))
	// Only the confirmation can be written.
	env.Stdout = &writerFailingAfter{allowed: 1}

	code, err := executeWithTimeout(t, cmd, env)

	if code != exitcode.General || !errors.Is(err, errTestWrite) {
		t.Errorf("Execute() with a failing write of the message = %d, %v, want %d, %v", code, err, exitcode.General, errTestWrite)
	}
	recvTestData(t, server)
	want := newSubscriberData(t, env, protocol.ConsumerDisconnect, protocol.NewConsumerDisconnectPayload(string(cmd.subscriber)))
	if got := recvTestData(t, server); !reflect.DeepEqual(got, want) {
		t.Errorf("request of Execute() after a failing write = %+v, want %+v", got, want)
	}
}

func TestSubscribeExecuteReturnsIPCCodeOnServerShutdown(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t), newServerData(t, protocol.ServerShutdown, protocol.NewServerShutdownPayload()))

	code, err := executeWithTimeout(t, cmd, env)

	if code != exitcode.IPC || !errors.Is(err, errServerShuttingDown) {
		t.Errorf("Execute() after a server shutdown = %d, %v, want %d, %v", code, err, exitcode.IPC, errServerShuttingDown)
	}
	recvTestData(t, server)
	assertNoTestData(t, server)
}

func TestSubscribeExecuteKeepsWaitingAfterRecvTimeout(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, stdout := newSubscribeEnv(t, newSubscriptionReply(t))
	env.RecvTimeout = shortRecvTimeout
	shutdown, requestShutdown := newShutdown()
	env.Shutdown = shutdown

	result := startExecute(cmd, env)
	recvTestData(t, server)
	time.Sleep(5 * shortRecvTimeout)
	sendTestReplies(t, env, newDeliveryData(t, 0, "a", "1"))
	recvTestData(t, server)
	requestShutdown()

	if code, err := waitExecute(t, result); err != nil || code != exitcode.Success {
		t.Fatalf("Execute() after a late delivery and a shutdown = %d, %v, want %d, nil", code, err, exitcode.Success)
	}
	if got, want := stdout.String(), fmt.Sprintf("subscribed to %s\na:1\n", cmd.topic); got != want {
		t.Errorf("Execute() after a late delivery wrote %q, want %q", got, want)
	}
}

func TestSubscribeExecuteReturnsConfirmationWriteError(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t))
	env.Stdout = failingWriter{}

	code, err := executeWithTimeout(t, cmd, env)

	if code != exitcode.General || !errors.Is(err, errTestWrite) {
		t.Errorf("Execute() with a failing write of the confirmation = %d, %v, want %d, %v", code, err, exitcode.General, errTestWrite)
	}
	recvTestData(t, server)
	want := newSubscriberData(t, env, protocol.ConsumerDisconnect, protocol.NewConsumerDisconnectPayload(string(cmd.subscriber)))
	if got := recvTestData(t, server); !reflect.DeepEqual(got, want) {
		t.Errorf("request of Execute() after a failing write of the confirmation = %+v, want %+v", got, want)
	}
}

func TestSubscribeExecuteReturnsIPCCodeWhenSubscriptionCannotBeSent(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t))
	fillTestQueue(t, server.IPCPath())
	env.SendTimeout = shortSendTimeout

	code, err := executeWithTimeout(t, cmd, env)

	if code != exitcode.IPC || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Execute() with a full server queue = %d, %v, want %d, %v", code, err, exitcode.IPC, context.DeadlineExceeded)
	}
}

func TestSubscribeExecuteReturnsIPCCodeWhenAcknowledgementCannotBeSent(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, stdout := newSubscribeEnv(t, newSubscriptionReply(t))
	env.SendTimeout = shortSendTimeout

	result := startExecute(cmd, env)
	recvTestData(t, server)
	// Execute sends nothing else before the delivery, so the queue stays full.
	fillTestQueue(t, server.IPCPath())
	sendTestReplies(t, env, newDeliveryData(t, 0, "a", "1"))

	code, err := waitExecute(t, result)

	if code != exitcode.IPC || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Execute() with a full server queue at acknowledgement = %d, %v, want %d, %v", code, err, exitcode.IPC, context.DeadlineExceeded)
	}
	if got, want := stdout.String(), fmt.Sprintf("subscribed to %s\na:1\n", cmd.topic); got != want {
		t.Errorf("Execute() with a full server queue at acknowledgement wrote %q, want %q", got, want)
	}
}

func TestSubscribeExecuteReturnsDisconnectError(t *testing.T) {
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t))
	env.SendTimeout = shortSendTimeout
	shutdown, requestShutdown := newShutdown()
	env.Shutdown = shutdown

	result := startExecute(cmd, env)
	recvTestData(t, server)
	// Execute sends nothing else before the shutdown, so the queue stays full.
	fillTestQueue(t, server.IPCPath())
	requestShutdown()

	code, err := waitExecute(t, result)

	if code != exitcode.IPC || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Execute() with a full server queue at disconnection = %d, %v, want %d, %v", code, err, exitcode.IPC, context.DeadlineExceeded)
	}
}

func TestSubscribeExecuteStopsWhenContextDeadlineExpires(t *testing.T) {
	const deadline = 200 * time.Millisecond
	cmd := &subscribeCommand{topic: "orders", subscriber: "client0"}
	env, server, _ := newSubscribeEnv(t, newSubscriptionReply(t))
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	result := make(chan executeResult, 1)
	go func() {
		code, err := cmd.Execute(ctx, env)
		result <- executeResult{code: code, err: err}
	}()
	code, err := waitExecute(t, result)

	if code != exitcode.IPC || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Execute() with a context expiring after %v = %d, %v, want %d, %v", deadline, code, err, exitcode.IPC, context.DeadlineExceeded)
	}
	recvTestData(t, server)
	assertNoTestData(t, server)
}
