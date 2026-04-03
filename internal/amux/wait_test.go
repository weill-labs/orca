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

func TestWaitReturnsContextErrorImmediatelyWhenDelayIsNonPositive(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Wait(ctx, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want %v", err, context.Canceled)
	}
}

func TestWaitReturnsNilWhenDelayIsNonPositive(t *testing.T) {
	t.Parallel()

	if err := Wait(context.Background(), 0); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func TestWaitReturnsNilAfterDelay(t *testing.T) {
	t.Parallel()

	if err := Wait(context.Background(), 5*time.Millisecond); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func TestWaitUntilReturnsDeadlineExceeded(t *testing.T) {
	t.Parallel()

	err := WaitUntil(context.Background(), time.Now().Add(-time.Millisecond), 50*time.Millisecond)
	if !errors.Is(err, ErrWaitDeadlineExceeded) {
		t.Fatalf("WaitUntil() error = %v, want %v", err, ErrWaitDeadlineExceeded)
	}
}

func TestWaitUntilReturnsNilBeforeDeadline(t *testing.T) {
	t.Parallel()

	err := WaitUntil(context.Background(), time.Now().Add(20*time.Millisecond), 5*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitUntil() error = %v", err)
	}
}

func TestWaitUntilClampsToRemainingTime(t *testing.T) {
	t.Parallel()

	err := WaitUntil(context.Background(), time.Now().Add(20*time.Millisecond), time.Second)
	if err != nil {
		t.Fatalf("WaitUntil() error = %v", err)
	}
}
