package commands

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc"
	"tree_nity/src/ipc/id"
	"tree_nity/src/ipc/protocol"
)

// newCreateEnv returns an Env whose inbound already holds the reply, with the
// queue of the server and the standard output of the command.
func newCreateEnv(t *testing.T, reply protocol.Data) (Env, *ipc.Inbound, *bytes.Buffer) {
	t.Helper()
	server := newTestInbound(t, testIPCPath(t, "server"))
	env := newReplyEnv(t, reply)
	env.Outbound = newTestOutbound(t, server.IPCPath())
	stdout := &bytes.Buffer{}
	env.Stdout = stdout
	return env, server, stdout
}

func TestParseCreateReadsTopic(t *testing.T) {
	args := []string{"orders"}

	command, err := ParseCreate(args)

	if err != nil {
		t.Fatalf("ParseCreate(%q) error = %v, want nil", args, err)
	}
	cmd, ok := command.(*createCommand)
	if !ok {
		t.Fatalf("ParseCreate(%q) = %T, want *createCommand", args, command)
	}
	if want := id.ID(args[0]); cmd.topic != want {
		t.Errorf("ParseCreate(%q) topic = %q, want %q", args, cmd.topic, want)
	}
}

func TestParseCreateRejectsMissingTopic(t *testing.T) {
	args := []string{}

	if _, err := ParseCreate(args); !errors.Is(err, errMissingArgument) {
		t.Errorf("ParseCreate(%q) error = %v, want %v", args, err, errMissingArgument)
	}
}

func TestParseCreateRejectsInvalidTopic(t *testing.T) {
	args := []string{"bad topic"}

	if _, err := ParseCreate(args); !errors.Is(err, id.ErrInvalidID) {
		t.Errorf("ParseCreate(%q) error = %v, want %v", args, err, id.ErrInvalidID)
	}
}

func TestParseCreateRejectsExtraArgument(t *testing.T) {
	args := []string{"orders", "extra"}

	if _, err := ParseCreate(args); !errors.Is(err, errTooManyArguments) {
		t.Errorf("ParseCreate(%q) error = %v, want %v", args, err, errTooManyArguments)
	}
}

func TestCreateExecuteSendsCreateTopicRequest(t *testing.T) {
	cmd := &createCommand{topic: "orders"}
	env, server, _ := newCreateEnv(t, newServerData(t, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload()))

	if code, err := cmd.Execute(context.Background(), env); err != nil {
		t.Fatalf("Execute() for topic %q = %d, %v, want %d, nil", cmd.topic, code, err, exitcode.Success)
	}

	want, err := protocol.NewData(
		protocol.CreateTopic,
		protocol.NewClientMetadata(env.Inbound.IPCPath()),
		protocol.NewCreateTopicPayload(string(cmd.topic)),
	)
	if err != nil {
		t.Fatalf("NewData(%d) error = %v, want nil", protocol.CreateTopic, err)
	}
	if got := recvTestData(t, server); !reflect.DeepEqual(got, want) {
		t.Errorf("request of Execute() for topic %q = %+v, want %+v", cmd.topic, got, want)
	}
}

func TestCreateExecutePrintsConfirmation(t *testing.T) {
	cmd := &createCommand{topic: "orders"}
	env, _, stdout := newCreateEnv(t, newServerData(t, protocol.CreateTopicReply, protocol.NewCreateTopicReplyPayload()))

	code, err := cmd.Execute(context.Background(), env)

	if err != nil || code != exitcode.Success {
		t.Fatalf("Execute() for topic %q = %d, %v, want %d, nil", cmd.topic, code, err, exitcode.Success)
	}
	if got, want := stdout.String(), "topic created\n"; got != want {
		t.Errorf("Execute() for topic %q wrote %q, want %q", cmd.topic, got, want)
	}
}

func TestCreateExecuteReturnsTopicCodeWhenTopicExists(t *testing.T) {
	cmd := &createCommand{topic: "orders"}
	reply := protocol.NewErrorReplyPayload(protocol.TopicExists, "topic exists")
	env, _, stdout := newCreateEnv(t, newServerData(t, protocol.ErrorReply, reply))

	code, err := cmd.Execute(context.Background(), env)

	if code != exitcode.Topic || !errors.Is(err, errRequestRejected) {
		t.Errorf("Execute() for existing topic %q = %d, %v, want %d, %v", cmd.topic, code, err, exitcode.Topic, errRequestRejected)
	}
	if stdout.Len() != 0 {
		t.Errorf("Execute() for existing topic %q wrote %q, want nothing", cmd.topic, stdout.String())
	}
}
