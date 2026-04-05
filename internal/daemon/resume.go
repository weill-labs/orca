package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

func (d *Daemon) Resume(ctx context.Context, issue, prompt string) error {
	if err := d.requireStarted(); err != nil {
		return err
	}

	task, err := d.state.TaskByIssue(ctx, d.project, issue)
	if err != nil {
		return err
	}
	if !taskCanResume(task.Status) {
		return fmt.Errorf("task %s with status %s cannot be resumed", issue, task.Status)
	}

	profile, err := d.profileForTask(ctx, task)
	if err != nil {
		return fmt.Errorf("load agent profile %q: %w", task.AgentProfile, err)
	}

	worker, hasWorker, err := d.resumeWorker(ctx, task)
	if err != nil {
		return err
	}

	paneID := strings.TrimSpace(task.PaneID)
	if paneID != "" {
		exists, err := d.amux.PaneExists(ctx, paneID)
		if err != nil {
			return fmt.Errorf("check pane %s: %w", paneID, err)
		}
		if exists {
			return d.resumeExistingPane(ctx, task, worker, hasWorker, profile, strings.TrimSpace(prompt))
		}
	}

	return d.resumeWithFreshPane(ctx, task, worker, hasWorker, profile, strings.TrimSpace(prompt))
}

func taskCanResume(status string) bool {
	switch status {
	case TaskStatusActive, TaskStatusCancelled, TaskStatusFailed:
		return true
	default:
		return false
	}
}

func (d *Daemon) resumeWorker(ctx context.Context, task Task) (Worker, bool, error) {
	paneID := strings.TrimSpace(task.PaneID)
	if paneID == "" {
		return Worker{}, false, nil
	}

	worker, err := d.state.WorkerByPane(ctx, d.project, paneID)
	if errors.Is(err, ErrWorkerNotFound) {
		return Worker{}, false, nil
	}
	if err != nil {
		return Worker{}, false, fmt.Errorf("load worker %s: %w", paneID, err)
	}
	return worker, true, nil
}

func (d *Daemon) resumeExistingPane(ctx context.Context, task Task, worker Worker, hasWorker bool, profile AgentProfile, prompt string) error {
	paneID := strings.TrimSpace(task.PaneID)
	if paneID == "" {
		return fmt.Errorf("task %s has no worker pane", task.Issue)
	}
	if err := d.startAgentInPane(ctx, paneID, profile); err != nil {
		return err
	}

	metadata, err := d.assignmentPaneMetadata(ctx, paneID, profile.Name, task.Branch, task.Issue, resolveTaskTitle(task.Issue, task.Issue))
	if err != nil {
		return fmt.Errorf("build pane metadata: %w", err)
	}
	if err := d.setPaneMetadata(ctx, paneID, metadata); err != nil {
		return fmt.Errorf("set pane metadata: %w", err)
	}

	promptToSend := prompt
	if promptToSend == "" && (task.Status != TaskStatusActive || !hasWorker) {
		promptToSend = strings.TrimSpace(task.Prompt)
	}
	if promptToSend != "" {
		if err := d.sendPromptAndEnter(ctx, paneID, promptToSend); err != nil {
			return fmt.Errorf("send prompt: %w", err)
		}
		if prompt != "" {
			task.Prompt = promptToSend
		}
	}

	if task.Status != TaskStatusActive {
		if err := d.setIssueStatus(ctx, task.Issue, IssueStateInProgress); err != nil {
			return fmt.Errorf("set issue status: %w", err)
		}
	}

	return d.storeResumedTask(ctx, task, worker, hasWorker, Pane{ID: paneID, Name: task.PaneName})
}

func (d *Daemon) resumeWithFreshPane(ctx context.Context, task Task, worker Worker, hasWorker bool, profile AgentProfile, prompt string) error {
	// Resume reuses the recorded clone path instead of re-acquiring from the pool so the task continues in its existing checkout.
	clonePath := strings.TrimSpace(task.ClonePath)
	if clonePath == "" {
		return fmt.Errorf("task %s has no clone path", task.Issue)
	}

	pane, err := d.amux.Spawn(ctx, SpawnRequest{
		Session: d.session,
		AtPane:  d.leadPane,
		Name:    "worker-" + task.Issue,
		CWD:     clonePath,
		Command: profile.StartCommand,
	})
	if err != nil {
		return fmt.Errorf("spawn pane: %w", err)
	}

	cleanupCtx := context.WithoutCancel(ctx)
	spawned := true
	defer func() {
		if spawned {
			_ = d.amux.KillPane(cleanupCtx, pane.ID)
		}
	}()

	metadata, err := d.assignmentPaneMetadata(ctx, pane.ID, profile.Name, task.Branch, task.Issue, resolveTaskTitle(task.Issue, task.Issue))
	if err != nil {
		return fmt.Errorf("build pane metadata: %w", err)
	}
	if err := d.setPaneMetadata(ctx, pane.ID, metadata); err != nil {
		return fmt.Errorf("set pane metadata: %w", err)
	}
	if err := d.agentHandshake(ctx, pane.ID, profile); err != nil {
		return fmt.Errorf("agent handshake: %w", err)
	}

	promptToSend := prompt
	if promptToSend == "" {
		promptToSend = strings.TrimSpace(task.Prompt)
	}
	if promptToSend != "" {
		if err := d.sendPromptAndEnter(ctx, pane.ID, promptToSend); err != nil {
			return fmt.Errorf("send prompt: %w", err)
		}
		if prompt != "" {
			task.Prompt = promptToSend
		}
	}

	if err := d.setIssueStatus(ctx, task.Issue, IssueStateInProgress); err != nil {
		return fmt.Errorf("set issue status: %w", err)
	}

	if err := d.storeResumedTask(ctx, task, worker, hasWorker, pane); err != nil {
		return err
	}

	spawned = false
	return nil
}

func (d *Daemon) storeResumedTask(ctx context.Context, task Task, worker Worker, hasWorker bool, pane Pane) error {
	now := d.now()
	oldPaneID := task.PaneID

	task.Status = TaskStatusActive
	task.PaneID = pane.ID
	task.PaneName = pane.Name
	if task.PaneName == "" {
		task.PaneName = pane.ID
	}
	if task.CloneName == "" && task.ClonePath != "" {
		task.CloneName = filepath.Base(task.ClonePath)
	}
	task.UpdatedAt = now

	resumedWorker := worker
	if !hasWorker {
		resumedWorker = Worker{
			Project:      d.project,
			Health:       WorkerHealthHealthy,
			AgentProfile: task.AgentProfile,
			ClonePath:    task.ClonePath,
			Issue:        task.Issue,
		}
	}
	resumedWorker.Project = d.project
	resumedWorker.PaneID = pane.ID
	resumedWorker.PaneName = task.PaneName
	resumedWorker.Issue = task.Issue
	resumedWorker.ClonePath = task.ClonePath
	resumedWorker.AgentProfile = task.AgentProfile
	resumedWorker.Health = WorkerHealthHealthy
	resumedWorker.NudgeCount = 0
	resumedWorker.LastCapture = ""
	resumedWorker.LastActivityAt = now
	resumedWorker.UpdatedAt = now

	if oldPaneID != "" && oldPaneID != pane.ID {
		if err := d.state.DeleteWorker(ctx, d.project, oldPaneID); err != nil && !errors.Is(err, ErrWorkerNotFound) {
			return fmt.Errorf("delete worker after resume: %w", err)
		}
	}
	if err := d.state.PutWorker(ctx, resumedWorker); err != nil {
		return fmt.Errorf("store worker after resume: %w", err)
	}
	if err := d.state.PutTask(ctx, task); err != nil {
		return fmt.Errorf("store task after resume: %w", err)
	}

	return nil
}
