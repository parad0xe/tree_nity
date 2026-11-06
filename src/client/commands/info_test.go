package commands

import (
	"errors"
	"testing"

	"tree_nity/src/ipc/id"
)

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
