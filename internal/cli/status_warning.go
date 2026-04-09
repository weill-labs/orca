package cli

import (
	"encoding/json"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

type gitHubRateLimitStatusPayload struct {
	GitHubRateLimitedUntil time.Time `json:"github_rate_limited_until"`
}

func taskGitHubRateLimitWarning(taskStatus state.TaskStatus, now time.Time) string {
	for i := len(taskStatus.Events) - 1; i >= 0; i-- {
		event := taskStatus.Events[i]
		if event.Kind != "pr.rate_limited" || len(event.Payload) == 0 {
			continue
		}

		var payload gitHubRateLimitStatusPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			continue
		}
		if payload.GitHubRateLimitedUntil.IsZero() || !payload.GitHubRateLimitedUntil.After(now) {
			continue
		}

		return formatGitHubRateLimitWarning(payload.GitHubRateLimitedUntil)
	}
	return ""
}

func formatGitHubRateLimitWarning(until time.Time) string {
	return "github: rate limited until " + until.UTC().Format("15:04")
}
