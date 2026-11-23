package commands

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
)

// newConsumerInfoReply returns the reply of the server with the metadata of a subscriber.
func newConsumerInfoReply(t *testing.T, client, topic string, offset uint32, prefix, ipc string) protocol.Data {
	t.Helper()
	return newServerData(t, protocol.ConsumerInfoReply, protocol.NewConsumerInfoReplyPayload(client, topic, offset, prefix, ipc))
}

func TestParseInfoReadsSubscriber(t *testing.T) {
	args := []string{"client0"}

	command, err := ParseInfo(args)

	if err != nil {
		t.Fatalf("ParseInfo(%q) error = %v, want nil", args, err)
	}
	info, ok := command.(*infoCommand)
	if !ok {
		t.Fatalf("ParseInfo(%q) = %T, want *infoCommand", args, command)
	}
	if want := id.ID(args[0]); info.subscriber != want {
		t.Errorf("ParseInfo(%q) subscriber = %q, want %q", args, info.subscriber, want)
	}
}

func TestParseInfoRejectsMissingSubscriber(t *testing.T) {
	args := []string{}

	if _, err := ParseInfo(args); !errors.Is(err, errMissingArgument) {
		t.Errorf("ParseInfo(%q) error = %v, want %v", args, err, errMissingArgument)
	}
}

func TestParseInfoRejectsInvalidSubscriber(t *testing.T) {
	args := []string{"bad subscriber"}

	if _, err := ParseInfo(args); !errors.Is(err, id.ErrInvalidID) {
		t.Errorf("ParseInfo(%q) error = %v, want %v", args, err, id.ErrInvalidID)
	}
}

func TestParseInfoRejectsExtraArgument(t *testing.T) {
	args := []string{"client0", "extra"}

	if _, err := ParseInfo(args); !errors.Is(err, errTooManyArguments) {
		t.Errorf("ParseInfo(%q) error = %v, want %v", args, err, errTooManyArguments)
	}
}

func TestInfoExecuteSendsConsumerInfoRequest(t *testing.T) {
	cmd := &infoCommand{subscriber: "client0"}
	env, server, _ := newCommandEnv(t, newConsumerInfoReply(t, "client0", "orders", 0, "", "/client0"))

	if code, err := cmd.Execute(context.Background(), env); err != nil {
		t.Fatalf("Execute() for subscriber %q = %d, %v, want %d, nil", cmd.subscriber, code, err, exitcode.Success)
	}

	want, err := protocol.NewData(
		protocol.ConsumerInfo,
		protocol.NewClientMetadata(env.Inbound.IPCPath()),
		protocol.NewConsumerInfoPayload(string(cmd.subscriber)),
	)
	if err != nil {
		t.Fatalf("NewData(%d) error = %v, want nil", protocol.ConsumerInfo, err)
	}
	if got := recvTestData(t, server); !reflect.DeepEqual(got, want) {
		t.Errorf("request of Execute() for subscriber %q = %+v, want %+v", cmd.subscriber, got, want)
	}
}

func TestInfoExecutePrintsMetadataAsJSON(t *testing.T) {
	cmd := &infoCommand{subscriber: "client0"}
	client, topic, offset, prefix, ipc := "client0", "user_events", uint32(4), "user.create", "/tmp/operator.server.1337.client0"
	env, _, stdout := newCommandEnv(t, newConsumerInfoReply(t, client, topic, offset, prefix, ipc))

	code, err := cmd.Execute(context.Background(), env)

	if err != nil || code != exitcode.Success {
		t.Fatalf("Execute() for subscriber %q = %d, %v, want %d, nil", cmd.subscriber, code, err, exitcode.Success)
	}
	want := fmt.Sprintf(`{"client":%q,"topic":%q,"offset":%d,"prefix":%q,"ipc":%q}`+"\n", client, topic, offset, prefix, ipc)
	if got := stdout.String(); got != want {
		t.Errorf("Execute() for subscriber %q wrote %q, want %q", cmd.subscriber, got, want)
	}
}

func TestInfoExecuteDoesNotEscapeHTMLCharacters(t *testing.T) {
	cmd := &infoCommand{subscriber: "client0"}
	prefix := "a&b<c>"
	env, _, stdout := newCommandEnv(t, newConsumerInfoReply(t, "client0", "orders", 0, prefix, "/client0"))

	code, err := cmd.Execute(context.Background(), env)

	if err != nil || code != exitcode.Success {
		t.Fatalf("Execute() with prefix %q = %d, %v, want %d, nil", prefix, code, err, exitcode.Success)
	}
	want := fmt.Sprintf(`{"client":"client0","topic":"orders","offset":0,"prefix":%q,"ipc":"/client0"}`+"\n", prefix)
	if got := stdout.String(); got != want {
		t.Errorf("Execute() with prefix %q wrote %q, want %q", prefix, got, want)
	}
}

func TestInfoExecuteReturnsTopicCodeWhenClientIsMissing(t *testing.T) {
	cmd := &infoCommand{subscriber: "client0"}
	reply := protocol.NewErrorReplyPayload(protocol.ClientNotFound, "client not found")
	env, _, stdout := newCommandEnv(t, newServerData(t, protocol.ErrorReply, reply))

	code, err := cmd.Execute(context.Background(), env)

	if code != exitcode.Topic || !errors.Is(err, errRequestRejected) {
		t.Errorf("Execute() for missing subscriber %q = %d, %v, want %d, %v", cmd.subscriber, code, err, exitcode.Topic, errRequestRejected)
	}
	if stdout.Len() != 0 {
		t.Errorf("Execute() for missing subscriber %q wrote %q, want nothing", cmd.subscriber, stdout.String())
	}
}

func TestInfoExecuteReturnsWriteError(t *testing.T) {
	cmd := &infoCommand{subscriber: "client0"}
	env, _, _ := newCommandEnv(t, newConsumerInfoReply(t, "client0", "orders", 0, "", "/client0"))
	env.Stdout = failingWriter{}

	code, err := cmd.Execute(context.Background(), env)

	if code != exitcode.General || !errors.Is(err, errTestWrite) {
		t.Errorf("Execute() with a failing write = %d, %v, want %d, %v", code, err, exitcode.General, errTestWrite)
	}
}
