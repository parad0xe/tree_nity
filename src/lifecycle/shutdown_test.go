package lifecycle_test

import (
	"testing"

	"tree_nity/src/lifecycle"
)

func TestGracefulShutdownNotRequestedByDefault(t *testing.T) {
	var shutdown lifecycle.GracefulShutdown
	if got := shutdown.IsRequested(); got {
		t.Errorf("IsRequested() on the zero value = %v, want %v", got, false)
	}
}

func TestGracefulShutdownNotRequestedWhileOpen(t *testing.T) {
	shutdown := lifecycle.NewGracefulShutdown(make(chan struct{}))
	if got := shutdown.IsRequested(); got {
		t.Errorf("IsRequested() with an open channel = %v, want %v", got, false)
	}
}

func TestGracefulShutdownRequestedWhenClosed(t *testing.T) {
	requestedChan := make(chan struct{})
	close(requestedChan)
	shutdown := lifecycle.NewGracefulShutdown(requestedChan)
	if got := shutdown.IsRequested(); !got {
		t.Errorf("IsRequested() with a closed channel = %v, want %v", got, true)
	}
}

func TestGracefulShutdownChannelOpenWhileNotRequested(t *testing.T) {
	shutdown := lifecycle.NewGracefulShutdown(make(chan struct{}))
	select {
	case <-shutdown.Requested():
		t.Errorf("Requested() with an open channel is closed, want open")
	default:
	}
}

func TestGracefulShutdownChannelClosedWhenRequested(t *testing.T) {
	requestedChan := make(chan struct{})
	close(requestedChan)
	shutdown := lifecycle.NewGracefulShutdown(requestedChan)
	select {
	case <-shutdown.Requested():
	default:
		t.Errorf("Requested() with a closed channel is open, want closed")
	}
}
