package daemon

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestGitHubRateLimitErrorNilReceiver(t *testing.T) {
	t.Parallel()

	var err *gitHubRateLimitError
	if got := err.Error(); got != "" {
		t.Fatalf("Error() = %q, want empty string", got)
	}
	if got := err.Unwrap(); got != nil {
		t.Fatalf("Unwrap() = %v, want nil", got)
	}
	if got := err.RateLimitedUntil(); !got.IsZero() {
		t.Fatalf("RateLimitedUntil() = %v, want zero time", got)
	}
}

func TestGitHubRateLimitUntil(t *testing.T) {
	t.Parallel()

	until := time.Date(2026, 4, 2, 9, 2, 0, 0, time.UTC)

	tests := []struct {
		name    string
		err     error
		want    time.Time
		wantOK  bool
	}{
		{name: "plain error", err: errors.New("gh failed")},
		{name: "zero deadline", err: &gitHubRateLimitError{err: errors.New("gh: HTTP 429")}},
		{
			name:   "wrapped rate limit error",
			err:    fmt.Errorf("wrap: %w", &gitHubRateLimitError{err: errors.New("gh: HTTP 429"), until: until}),
			want:   until,
			wantOK: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := gitHubRateLimitUntil(tt.err)
			if ok != tt.wantOK {
				t.Fatalf("gitHubRateLimitUntil() ok = %v, want %v", ok, tt.wantOK)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("gitHubRateLimitUntil() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAppendGitHubRateLimitEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)
	until := now.Add(2 * time.Minute)
	d := &Daemon{
		now: func() time.Time { return now },
	}

	baseUpdate := TaskStateUpdate{
		Active: ActiveAssignment{
			Task: Task{
				Project:      "/tmp/project",
				Issue:        "LAB-986",
				PaneID:       "pane-1",
				CloneName:    "clone-01",
				ClonePath:    "/tmp/project/.orca/pool/clone-01",
				Branch:       "LAB-986",
				AgentProfile: "codex",
				PRNumber:     42,
			},
		},
	}
	profile := AgentProfile{Name: "codex"}

	t.Run("ignores non rate limit errors", func(t *testing.T) {
		t.Parallel()

		update := baseUpdate
		if ok := d.appendGitHubRateLimitEvent(&update, profile, errors.New("gh failed")); ok {
			t.Fatal("appendGitHubRateLimitEvent() = true, want false")
		}
		if got := len(update.Events); got != 0 {
			t.Fatalf("len(update.Events) = %d, want 0", got)
		}
	})

	t.Run("records event with retry metadata", func(t *testing.T) {
		t.Parallel()

		update := baseUpdate
		err := &gitHubRateLimitError{
			err:   errors.New("gh: HTTP 429"),
			until: until,
		}

		if ok := d.appendGitHubRateLimitEvent(&update, profile, err); !ok {
			t.Fatal("appendGitHubRateLimitEvent() = false, want true")
		}
		if got, want := len(update.Events), 1; got != want {
			t.Fatalf("len(update.Events) = %d, want %d", got, want)
		}

		event := update.Events[0]
		if got, want := event.Type, EventPRRateLimited; got != want {
			t.Fatalf("event.Type = %q, want %q", got, want)
		}
		if got, want := event.Message, formatGitHubRateLimitWarning(until); got != want {
			t.Fatalf("event.Message = %q, want %q", got, want)
		}
		if !event.GitHubRateLimitedUntil.Equal(until) {
			t.Fatalf("event.GitHubRateLimitedUntil = %v, want %v", event.GitHubRateLimitedUntil, until)
		}
	})
}

