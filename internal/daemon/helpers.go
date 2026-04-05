package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type trackedStatus string

const (
	trackedStatusActive    trackedStatus = "active"
	trackedStatusCompleted trackedStatus = "completed"
)

type trackedIssueRef struct {
	ID     string        `json:"id"`
	Status trackedStatus `json:"status,omitempty"`
}

type trackedPRRef struct {
	Number int           `json:"number"`
	Status trackedStatus `json:"status,omitempty"`
}

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

func assignmentMetadata(agentProfile, branch, task string) map[string]string {
	metadata := map[string]string{
		"agent_profile": agentProfile,
		"branch":        branch,
		"task":          task,
	}
	return metadata
}

func trackedIssueMetadata(issue string, status trackedStatus) map[string]string {
	issue = strings.TrimSpace(issue)
	if issue == "" {
		return nil
	}

	data, err := json.Marshal([]trackedIssueRef{{
		ID:     issue,
		Status: status,
	}})
	if err != nil {
		return nil
	}
	return map[string]string{"tracked_issues": string(data)}
}

func trackedPRMetadata(prNumber int, status trackedStatus) map[string]string {
	if prNumber <= 0 {
		return nil
	}

	data, err := json.Marshal([]trackedPRRef{{
		Number: prNumber,
		Status: status,
	}})
	if err != nil {
		return nil
	}
	return map[string]string{"tracked_prs": string(data)}
}

func taskCompletionMetadata(issue string, prNumber int, merged bool) map[string]string {
	metadata := mergeMetadata(
		map[string]string{"status": "done"},
		trackedIssueMetadata(issue, trackedStatusCompleted),
	)
	if merged {
		metadata = mergeMetadata(metadata, trackedPRMetadata(prNumber, trackedStatusCompleted))
	}
	return metadata
}

func mergeMetadata(parts ...map[string]string) map[string]string {
	var merged map[string]string
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		if merged == nil {
			merged = make(map[string]string)
		}
		for key, value := range part {
			merged[key] = value
		}
	}
	return merged
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

func (d *Daemon) sendPromptAndEnter(ctx context.Context, paneID, prompt string) error {
	trimmed := strings.TrimRight(prompt, "\r\n")
	if trimmed == "" {
		return errors.New("prompt is empty")
	}
	if err := d.amux.SendKeys(ctx, paneID, trimmed); err != nil {
		return err
	}
	if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
		return err
	}
	return d.amux.SendKeys(ctx, paneID, "Enter")
}

func (d *Daemon) startAgentInPane(ctx context.Context, paneID string, profile AgentProfile) error {
	if err := d.amux.SendKeys(ctx, paneID, profile.StartCommand, "Enter"); err != nil {
		return fmt.Errorf("restart agent in pane %s: %w", paneID, err)
	}
	if err := d.agentHandshake(ctx, paneID, profile); err != nil {
		return fmt.Errorf("agent handshake: %w", err)
	}
	return nil
}

func ensureTrailingNewline(input string) string {
	if strings.HasSuffix(input, "\n") {
		return input
	}
	return input + "\n"
}
