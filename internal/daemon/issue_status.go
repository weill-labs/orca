package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/weill-labs/orca/internal/linear"
)

func (d *Daemon) setIssueStatus(ctx context.Context, projectPath, issue, state string) error {
	if d.issueTracker == nil || isGitHubIssueIdentifier(issue) {
		return nil
	}
	err := d.issueTracker.SetIssueStatus(ctx, issue, state)
	if err == nil {
		return nil
	}
	if isLinearEntityNotFoundError(err) {
		d.emit(ctx, Event{
			Time:    d.now(),
			Type:    EventIssueStatusSkipped,
			Project: projectPath,
			Issue:   issue,
			Message: fmt.Sprintf("skipped Linear issue status update to %q: %v", state, err),
		})
		return nil
	}
	return err
}

func isLinearEntityNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, linear.ErrEntityNotFound) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "entity not found") && strings.Contains(message, "input_error")
}
