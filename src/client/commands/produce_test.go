package commands

import (
	"errors"
	"testing"

	"tree_nity/src/ipc/id"
)

func TestParseProduceReadsTopic(t *testing.T) {
	args := []string{"orders"}

	command, err := ParseProduce(args)

	if err != nil {
		t.Fatalf("ParseProduce(%q) error = %v, want nil", args, err)
	}
	p, ok := command.(*produce)
	if !ok {
		t.Fatalf("ParseProduce(%q) = %T, want *produce", args, command)
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
	p, ok := command.(*produce)
	if !ok {
		t.Fatalf("ParseProduce(%q) = %T, want *produce", args, command)
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
