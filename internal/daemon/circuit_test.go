package daemon

import (
	"context"
	"errors"
	"reflect"
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

func TestCircuitBreakerHooksEmitOnOpenAndCloseTransitions(t *testing.T) {
	t.Parallel()

	clock := &fakeClock{now: time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)}
	var transitions []string
	breaker := NewCircuitBreakerWithHooks(clock.Now, 3, 60*time.Second, CircuitBreakerHooks{
		OnOpen: func() {
			transitions = append(transitions, "open")
		},
		OnClose: func() {
			transitions = append(transitions, "close")
		},
	})

	for i := 0; i < 3; i++ {
		if err := breaker.Allow(); err != nil {
			t.Fatalf("Allow() before open error = %v", err)
		}
		breaker.RecordFailure()
	}

	clock.Advance(60 * time.Second)
	if err := breaker.Allow(); err != nil {
		t.Fatalf("Allow() after cooldown error = %v", err)
	}

	if got, want := transitions, []string{"open", "close"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("transitions = %#v, want %#v", got, want)
	}
}

func TestWithCircuitDoesNotRecordCanceledContextAsFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			breaker := NewCircuitBreaker(time.Now, 3, time.Minute)
			for i := 0; i < 3; i++ {
				_, err := withCircuit(breaker, func() (int, error) {
					return 0, tt.err
				})
				if !errors.Is(err, tt.err) {
					t.Fatalf("withCircuit() error = %v, want %v", err, tt.err)
				}
			}

			if err := breaker.Allow(); err != nil {
				t.Fatalf("Allow() after canceled errors = %v, want nil", err)
			}
		})
	}
}

func TestCircuitGitHubClientLookupPRReviewsSharesBreakerState(t *testing.T) {
	t.Parallel()

	breaker := NewCircuitBreaker(time.Now, 3, time.Minute)
	client := newCircuitGitHubClient(circuitGitHubClientStub{
		err: errors.New("github unavailable"),
	}, breaker)

	for i := 0; i < 3; i++ {
		_, _, err := client.lookupPRReviews(context.Background(), 42)
		if err == nil {
			t.Fatalf("lookupPRReviews() error on attempt %d = nil, want failure", i+1)
		}
	}

	if _, _, err := client.lookupPRReviews(context.Background(), 42); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("lookupPRReviews() after threshold error = %v, want %v", err, ErrCircuitBreakerOpen)
	}
}

type circuitGitHubClientStub struct {
	err error
}

func (c circuitGitHubClientStub) lookupPRNumber(context.Context, string) (int, error) {
	return 0, c.err
}

func (c circuitGitHubClientStub) lookupOpenPRNumber(context.Context, string) (int, error) {
	return 0, c.err
}

func (c circuitGitHubClientStub) isPRMerged(context.Context, int) (bool, error) {
	return false, c.err
}

func (c circuitGitHubClientStub) lookupPRReviews(context.Context, int) (prReviewPayload, bool, error) {
	return prReviewPayload{}, false, c.err
}
