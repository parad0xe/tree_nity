package commands

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"tree_nity/src/exitcode"
	"tree_nity/src/ipc/protocol"
)

func TestParseListReturnsListCommand(t *testing.T) {
	args := []string{}

	command, err := ParseList(args)

	if err != nil {
		t.Fatalf("ParseList(%q) error = %v, want nil", args, err)
	}
	if _, ok := command.(*listCommand); !ok {
		t.Errorf("ParseList(%q) = %T, want *listCommand", args, command)
	}
}

func TestParseListRejectsExtraArgument(t *testing.T) {
	args := []string{"extra"}

	if _, err := ParseList(args); !errors.Is(err, errTooManyArguments) {
		t.Errorf("ParseList(%q) error = %v, want %v", args, err, errTooManyArguments)
	}
}

func TestListExecuteSendsListTopicRequest(t *testing.T) {
	cmd := &listCommand{}
	env, server, _ := newCommandEnv(t, newServerData(t, protocol.ListTopicReply, protocol.NewListTopicReplyPayload(nil)))

	if code, err := cmd.Execute(context.Background(), env); err != nil {
		t.Fatalf("Execute() = %d, %v, want %d, nil", code, err, exitcode.Success)
	}

	want, err := protocol.NewData(protocol.ListTopic, protocol.NewClientMetadata(env.Inbound.IPCPath()), protocol.NewListTopicPayload())
	if err != nil {
		t.Fatalf("NewData(%d) error = %v, want nil", protocol.ListTopic, err)
	}
	if got := recvTestData(t, server); !reflect.DeepEqual(got, want) {
		t.Errorf("request of Execute() = %+v, want %+v", got, want)
	}
}

func TestListExecutePrintsTopics(t *testing.T) {
	cmd := &listCommand{}
	topics := []string{"user_events", "orders"}
	env, _, stdout := newCommandEnv(t, newServerData(t, protocol.ListTopicReply, protocol.NewListTopicReplyPayload(topics)))

	code, err := cmd.Execute(context.Background(), env)

	if err != nil || code != exitcode.Success {
		t.Fatalf("Execute() with topics %q = %d, %v, want %d, nil", topics, code, err, exitcode.Success)
	}
	if got, want := stdout.String(), "user_events,orders\n"; got != want {
		t.Errorf("Execute() with topics %q wrote %q, want %q", topics, got, want)
	}
}

func TestListExecutePrintsEmptyLineWithoutTopics(t *testing.T) {
	cmd := &listCommand{}
	env, _, stdout := newCommandEnv(t, newServerData(t, protocol.ListTopicReply, protocol.NewListTopicReplyPayload(nil)))

	code, err := cmd.Execute(context.Background(), env)

	if err != nil || code != exitcode.Success {
		t.Fatalf("Execute() without topics = %d, %v, want %d, nil", code, err, exitcode.Success)
	}
	if got, want := stdout.String(), "\n"; got != want {
		t.Errorf("Execute() without topics wrote %q, want %q", got, want)
	}
}
