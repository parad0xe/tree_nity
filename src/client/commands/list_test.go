package commands

import (
	"errors"
	"testing"
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
