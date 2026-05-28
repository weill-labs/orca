package daemon

import (
	"context"
	"errors"
	"strings"

	"github.com/weill-labs/orca/internal/linear"
)

func (d *Daemon) setIssueStatus(ctx context.Context, projectPath, issue, state string) error {
	if d.issueTracker == nil || isGitHubIssueIdentifier(issue) {
		return nil
	}
	integrations, err := d.integrationConfigForProject(projectPath)
	if err != nil {
		return err
	}
	if !integrations.linearEnabled() {
		return nil
	}
	linearIssue, ok := d.linearIssueIDForStatus(ctx, projectPath, issue, state)
	if !ok {
		return nil
	}
	err = d.issueTracker.SetIssueStatus(ctx, linearIssue, state)
	if err == nil {
		return nil
	}
	if isLinearEntityNotFoundError(err) {
		d.emitIssueStatusSkipped(ctx, projectPath, linearIssue, state, err.Error())
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
