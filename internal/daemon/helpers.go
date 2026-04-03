package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
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

func (d *Daemon) profileForTask(ctx context.Context, task Task) (AgentProfile, error) {
	profile, err := d.config.AgentProfile(ctx, task.AgentProfile)
	if err != nil {
		return AgentProfile{}, err
	}
	if profile.Name == "" {
		profile.Name = task.AgentProfile
	}
	return profile, nil
}

func (d *Daemon) mergeQueueEvent(active *ActiveAssignment, eventType string, prNumber int, message string, at time.Time) Event {
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

	event.Issue = active.Task.Issue
	event.PaneID = active.Task.PaneID
	event.PaneName = active.Task.PaneName
	if event.PaneName == "" {
		event.PaneName = active.Worker.PaneName
	}
	if event.PaneName == "" {
		event.PaneName = active.Task.PaneID
	}
	event.CloneName = active.Task.CloneName
	if event.CloneName == "" && active.Task.ClonePath != "" {
		event.CloneName = filepath.Base(active.Task.ClonePath)
	}
	event.ClonePath = active.Task.ClonePath
	event.Branch = active.Task.Branch
	event.AgentProfile = active.Task.AgentProfile
	return event
}

func (d *Daemon) requireStarted() error {
	if !d.started.Load() {
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
