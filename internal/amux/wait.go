package amux

import (
	"context"
	"errors"
	"time"
)

var ErrWaitDeadlineExceeded = errors.New("wait deadline exceeded")

// Wait blocks for the requested duration unless the context finishes first.
func Wait(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// WaitUntil waits for the polling interval, clamped to the deadline.
func WaitUntil(ctx context.Context, deadline time.Time, interval time.Duration) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return ErrWaitDeadlineExceeded
	}
	if interval <= 0 || remaining < interval {
		interval = remaining
	}

	return Wait(ctx, interval)
}
