package amux

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitReturnsContextErrorWhenCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Wait(ctx, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want %v", err, context.Canceled)
	}
}

func TestWaitUntilReturnsDeadlineExceeded(t *testing.T) {
	t.Parallel()

	err := WaitUntil(context.Background(), time.Now().Add(-time.Millisecond), 50*time.Millisecond)
	if !errors.Is(err, ErrWaitDeadlineExceeded) {
		t.Fatalf("WaitUntil() error = %v, want %v", err, ErrWaitDeadlineExceeded)
	}
}
