package daemon

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreakerOpensAfterThreeFailuresAndClosesAfterCooldown(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)}
	breaker := NewCircuitBreaker(clock.Now, 3, 60*time.Second)

	for i := 0; i < 2; i++ {
		if err := breaker.Allow(); err != nil {
			t.Fatalf("Allow() before threshold error = %v", err)
		}
		breaker.RecordFailure()
	}

	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() at threshold error = %v", err)
	}
	breaker.RecordFailure()

	if err := breaker.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("Allow() after threshold error = %v, want %v", err, ErrCircuitBreakerOpen)
	}

	clock.Advance(59 * time.Second)
	if err := breaker.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("Allow() before cooldown expiry error = %v, want %v", err, ErrCircuitBreakerOpen)
	}

	clock.Advance(1 * time.Second)
	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() after cooldown error = %v", err)
	}
}
