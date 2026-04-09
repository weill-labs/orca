package daemon

import (
	"errors"
	"fmt"
	"time"
)

type gitHubRateLimitError struct {
	err   error
	until time.Time
}

func (e *gitHubRateLimitError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *gitHubRateLimitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func (e *gitHubRateLimitError) RateLimitedUntil() time.Time {
	if e == nil {
		return time.Time{}
	}
	return e.until
}

func gitHubRateLimitUntil(err error) (time.Time, bool) {
	var rateLimited interface{ RateLimitedUntil() time.Time }
	if !errors.As(err, &rateLimited) {
		return time.Time{}, false
	}
	until := rateLimited.RateLimitedUntil()
	if until.IsZero() {
		return time.Time{}, false
	}
	return until, true
}

func formatGitHubRateLimitWarning(until time.Time) string {
	return fmt.Sprintf("github: rate limited until %s", until.UTC().Format("15:04"))
}

func (d *Daemon) appendGitHubRateLimitEvent(update *TaskStateUpdate, profile AgentProfile, err error) bool {
	until, ok := gitHubRateLimitUntil(err)
	if !ok {
		return false
	}

	event := d.assignmentEvent(update.Active, profile, EventPRRateLimited, formatGitHubRateLimitWarning(until))
	event.GitHubRateLimitedUntil = until
	update.Events = append(update.Events, event)
	return true
}
