package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

func (d *Daemon) rollbackAssignment(ctx context.Context, clone Clone, pane Pane, branch string) error {
	result := d.amux.KillPane(ctx, pane.ID)
	result = errors.Join(result, d.cleanupCloneAndRelease(ctx, clone, branch))
	return result
}

func (d *Daemon) cleanupCloneAndRelease(ctx context.Context, clone Clone, branch string) error {
	if clone.CurrentBranch == "" {
		clone.CurrentBranch = branch
	}
	if clone.AssignedTask == "" {
		clone.AssignedTask = branch
	}
	return d.pool.Release(ctx, d.project, clone)
}

func (d *Daemon) prepareClone(ctx context.Context, clonePath, branch string) error {
	commands := [][]string{
		{"checkout", "main"},
		{"pull"},
		{"checkout", "-B", branch},
	}
	for _, args := range commands {
		if _, err := d.commands.Run(ctx, clonePath, "git", args...); err != nil {
			return err
		}
	}
	return nil
}

func taskBlocksAssignment(status string) bool {
	switch status {
	case "", TaskStatusDone, TaskStatusCancelled, TaskStatusFailed:
		return false
	default:
		return true
	}
}

func assignmentMetadata(agentProfile, branch, issue string, prNumber int) map[string]string {
	metadata := map[string]string{
		"agent_profile": agentProfile,
		"branch":        branch,
		"issue":         issue,
		"task":          issue,
	}
	if prNumber > 0 {
		metadata["pr"] = fmt.Sprintf("%d", prNumber)
	}
	return metadata
}

func (d *Daemon) setPaneMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	return d.amux.SetMetadata(ctx, paneID, metadata)
}

func (d *Daemon) setIssueStatus(ctx context.Context, issue, state string) error {
	if d.issueTracker == nil {
		return nil
	}
	return d.issueTracker.SetIssueStatus(ctx, issue, state)
}

func (d *Daemon) assignment(issue string) (*assignment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	active, ok := d.assignments[issue]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if active.pending {
		return nil, fmt.Errorf("issue %s assignment is still starting", issue)
	}
	return active, nil
}

func (d *Daemon) assignmentByPRNumber(prNumber int) (*assignment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	active := d.assignmentByPRNumberLocked(prNumber)
	if active == nil {
		return nil, fmt.Errorf("PR #%d is not associated with an active assignment", prNumber)
	}
	return active, nil
}

func (d *Daemon) assignmentByPRNumberLocked(prNumber int) *assignment {
	for _, active := range d.assignments {
		if active.pending {
			continue
		}
		if active.prNumber == prNumber {
			return active
		}
	}
	return nil
}

func (d *Daemon) mergeQueueEvent(active *assignment, eventType string, prNumber int, message string, at time.Time) Event {
	event := Event{
		Time:     at,
		Type:     eventType,
		Project:  d.project,
		PRNumber: prNumber,
		Message:  message,
	}
	if active == nil {
		return event
	}

	event.Issue = active.task.Issue
	event.PaneID = active.pane.ID
	event.PaneName = active.pane.Name
	event.CloneName = active.clone.Name
	event.ClonePath = active.clone.Path
	event.Branch = active.task.Branch
	event.AgentProfile = active.profile.Name
	return event
}

func (d *Daemon) requireStarted() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.started {
		return ErrNotStarted
	}
	return nil
}

func (d *Daemon) emit(ctx context.Context, event Event) {
	if err := d.state.RecordEvent(ctx, event); err != nil {
		_ = err
	}
	if d.events != nil {
		if err := d.events.Emit(ctx, event); err != nil {
			_ = err
		}
	}
}

func ensureTrailingNewline(input string) string {
	if strings.HasSuffix(input, "\n") {
		return input
	}
	return input + "\n"
}
