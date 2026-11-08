package commands

import (
	"errors"
	"testing"

	"tree_nity/src/ipc/id"
)

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
