package commands

import (
	"errors"
	"testing"

	"tree_nity/src/ipc/id"
)

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
