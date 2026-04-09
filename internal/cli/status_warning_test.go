package cli

import (
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestTaskGitHubRateLimitWarning(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 9, 6, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		events []state.Event
		want   string
	}{
		{
			name: "returns latest active warning message",
			events: []state.Event{
				{
					Kind:    "pr.rate_limited",
					Message: "github: rate limited until 06:03",
					Payload: []byte(`{"github_rate_limited_until":"2026-04-09T06:03:00Z"}`),
				},
				{
					Kind:    "pr.rate_limited",
					Message: "github: rate limited until 06:05",
					Payload: []byte(`{"github_rate_limited_until":"2026-04-09T06:05:00Z"}`),
				},
			},
			want: "github: rate limited until 06:05",
		},
		{
			name: "skips expired warning",
			events: []state.Event{
				{
					Kind:    "pr.rate_limited",
					Message: "github: rate limited until 05:55",
					Payload: []byte(`{"github_rate_limited_until":"2026-04-09T05:55:00Z"}`),
				},
			},
		},
		{
			name: "skips invalid payload",
			events: []state.Event{
				{
					Kind:    "pr.rate_limited",
					Message: "github: rate limited until 06:05",
					Payload: []byte(`{`),
				},
			},
		},
		{
			name: "skips unrelated event",
			events: []state.Event{
				{
					Kind:    "task.assigned",
					Message: "assigned",
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := taskGitHubRateLimitWarning(state.TaskStatus{Events: tt.events}, now)
			if got != tt.want {
				t.Fatalf("taskGitHubRateLimitWarning() = %q, want %q", got, tt.want)
			}
		})
	}
}
