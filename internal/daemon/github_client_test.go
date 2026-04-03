package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestGitHubCLIClientAppliesMinimumIntervalBetweenCalls(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	clock := &fakeClock{now: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)}
	var sleeps []time.Duration
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "/tmp/project",
		commands:    commands,
		now:         clock.Now,
		sleep:       recordSleep(&sleeps, clock),
		minInterval: 500 * time.Millisecond,
		maxAttempts: 1,
	})

	commands.queue("gh", []string{"pr", "list", "--head", "LAB-697", "--json", "number"}, `[{"number":42}]`, nil)
	commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	ctx := context.Background()
	prNumber, err := client.lookupPRNumber(ctx, "LAB-697")
	if err != nil {
		t.Fatalf("lookupPRNumber() error = %v", err)
	}
	if got, want := prNumber, 42; got != want {
		t.Fatalf("lookupPRNumber() = %d, want %d", got, want)
	}

	merged, err := client.isPRMerged(ctx, 42)
	if err != nil {
		t.Fatalf("isPRMerged() error = %v", err)
	}
	if !merged {
		t.Fatal("isPRMerged() = false, want true")
	}

	if got, want := sleeps, []time.Duration{500 * time.Millisecond}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep durations = %#v, want %#v", got, want)
	}
}

func TestGitHubCLIClientRetriesRateLimitedRequestsWithBackoff(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	clock := &fakeClock{now: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)}
	var sleeps []time.Duration
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:        "/tmp/project",
		commands:       commands,
		now:            clock.Now,
		sleep:          recordSleep(&sleeps, clock),
		initialBackoff: time.Second,
		maxBackoff:     4 * time.Second,
		maxAttempts:    3,
	})

	args := []string{"pr", "view", "42", "--json", "mergedAt"}
	commands.queue("gh", args, ``, errors.New("gh: API rate limit exceeded for 203.0.113.42"))
	commands.queue("gh", args, ``, errors.New("gh: secondary rate limit: wait a few minutes before trying again"))
	commands.queue("gh", args, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	merged, err := client.isPRMerged(context.Background(), 42)
	if err != nil {
		t.Fatalf("isPRMerged() error = %v", err)
	}
	if !merged {
		t.Fatal("isPRMerged() = false, want true")
	}

	if got, want := sleeps, []time.Duration{time.Second, 2 * time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep durations = %#v, want %#v", got, want)
	}
	if got, want := commands.countCalls("gh", args), 3; got != want {
		t.Fatalf("gh call count = %d, want %d", got, want)
	}
}

func TestGitHubCLIClientDoesNotRetryNonRateLimitErrors(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	clock := &fakeClock{now: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)}
	var sleeps []time.Duration
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:        "/tmp/project",
		commands:       commands,
		now:            clock.Now,
		sleep:          recordSleep(&sleeps, clock),
		initialBackoff: time.Second,
		maxBackoff:     4 * time.Second,
		maxAttempts:    3,
	})

	args := []string{"pr", "list", "--head", "LAB-697", "--json", "number"}
	commands.queue("gh", args, ``, errors.New("gh: authentication failed"))

	_, err := client.lookupPRNumber(context.Background(), "LAB-697")
	if err == nil {
		t.Fatal("lookupPRNumber() succeeded, want error")
	}

	if got, want := sleeps, []time.Duration(nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep durations = %#v, want %#v", got, want)
	}
	if got, want := commands.countCalls("gh", args), 1; got != want {
		t.Fatalf("gh call count = %d, want %d", got, want)
	}
}

func recordSleep(sleeps *[]time.Duration, clock *fakeClock) func(context.Context, time.Duration) error {
	return func(_ context.Context, delay time.Duration) error {
		*sleeps = append(*sleeps, delay)
		clock.Advance(delay)
		return nil
	}
}
