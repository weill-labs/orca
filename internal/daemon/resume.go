package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

func (d *Daemon) Resume(ctx context.Context, issue, prompt string) error {
	return d.resume(ctx, d.project, issue, prompt)
}

func (d *Daemon) resume(ctx context.Context, projectPath, issue, prompt string) error {
	if err := d.requireStarted(); err != nil {
		return err
	}

	task, err := d.state.TaskByIssue(ctx, projectPath, issue)
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

	worker, hasWorker, err := d.resumeWorkerForProject(ctx, projectPath, task)
	if err != nil {
		return err
	}
	if hasWorker {
		if err := d.normalizeStoredPaneRef(ctx, &task, &worker); err != nil {
			return err
		}
	} else if err := d.normalizeStoredPaneRef(ctx, &task, nil); err != nil {
		return err
	}

	paneID := strings.TrimSpace(task.PaneID)
	if paneID != "" {
		exists, _, err := d.paneExists(ctx, paneID)
		if err != nil {
			return fmt.Errorf("check pane %s: %w", paneID, err)
		}
		if exists {
			return d.resumeExistingPaneForProject(ctx, projectPath, task, worker, hasWorker, profile, strings.TrimSpace(prompt))
		}
	}

	return d.resumeWithFreshPaneForProject(ctx, projectPath, task, worker, hasWorker, profile, strings.TrimSpace(prompt))
}

func taskCanResume(status string) bool {
	switch status {
	case TaskStatusActive, TaskStatusCancelled, TaskStatusFailed:
		return true
	default:
		return false
	}
}

func (d *Daemon) projectPathForTask(task Task) string {
	if task.Project != "" {
		return task.Project
	}
	return d.project
}

func (d *Daemon) resumeWorker(ctx context.Context, task Task) (Worker, bool, error) {
	return d.resumeWorkerForProject(ctx, d.projectPathForTask(task), task)
}

func (d *Daemon) resumeWorkerForProject(ctx context.Context, projectPath string, task Task) (Worker, bool, error) {
	workerID := stableWorkerRef(task, Worker{})
	if workerID != "" {
		worker, err := d.state.WorkerByID(ctx, projectPath, workerID)
		if err == nil {
			return worker, true, nil
		}
		if !errors.Is(err, ErrWorkerNotFound) {
			return Worker{}, false, fmt.Errorf("load worker %s: %w", workerID, err)
		}
	}

	paneID := strings.TrimSpace(task.PaneID)
	if paneID == "" {
		return Worker{}, false, nil
	}

	worker, err := d.state.WorkerByPane(ctx, projectPath, paneID)
	if errors.Is(err, ErrWorkerNotFound) {
		return Worker{}, false, nil
	}
	if err != nil {
		return Worker{}, false, fmt.Errorf("load worker %s: %w", paneID, err)
	}
	return worker, true, nil
}

func (d *Daemon) resumeExistingPane(ctx context.Context, task Task, worker Worker, hasWorker bool, profile AgentProfile, prompt string) error {
	return d.resumeExistingPaneForProject(ctx, d.projectPathForTask(task), task, worker, hasWorker, profile, prompt)
}

func (d *Daemon) resumeExistingPaneForProject(ctx context.Context, projectPath string, task Task, worker Worker, hasWorker bool, profile AgentProfile, prompt string) error {
	paneID := strings.TrimSpace(task.PaneID)
	if paneID == "" {
		return fmt.Errorf("task %s has no worker pane", task.Issue)
	}
	updatedWorker, err := d.captureCrashReportForRestart(ctx, task, worker, profile)
	if err != nil {
		return err
	}
	worker = updatedWorker
	startupSnapshot, err := d.startAgentInPane(ctx, paneID, profile)
	if err != nil {
		return err
	}
	worker.LastCapture = startupSnapshot.Output()

	metadata, err := d.assignmentPaneMetadata(ctx, projectPath, paneID, profile.Name, task.Branch, task.Issue, resolveTaskTitle(task.Issue, task.Issue), task.PRNumber)
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
		if err := d.setIssueStatus(ctx, projectPath, task.Issue, IssueStateInProgress); err != nil {
			return fmt.Errorf("set issue status: %w", err)
		}
	}

	return d.storeResumedTaskForProject(ctx, projectPath, task, worker, hasWorker, Pane{ID: paneID, Name: task.PaneName})
}

func (d *Daemon) resumeWithFreshPane(ctx context.Context, task Task, worker Worker, hasWorker bool, profile AgentProfile, prompt string) error {
	return d.resumeWithFreshPaneForProject(ctx, d.projectPathForTask(task), task, worker, hasWorker, profile, prompt)
}

func (d *Daemon) resumeWithFreshPaneForProject(ctx context.Context, projectPath string, task Task, worker Worker, hasWorker bool, profile AgentProfile, prompt string) error {
	// Resume reuses the recorded clone path instead of re-acquiring from the pool so the task continues in its existing checkout.
	clonePath := strings.TrimSpace(task.ClonePath)
	if clonePath == "" {
		return fmt.Errorf("task %s has no clone path", task.Issue)
	}

	stableRef := strings.TrimSpace(task.WorkerID)
	if stableRef == "" {
		stableRef = stableWorkerRef(task, worker)
	}
	pane, err := d.spawnWorkerPane(ctx, task, stableRef, clonePath, profile)
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

	metadata, err := d.assignmentPaneMetadata(ctx, projectPath, pane.ID, profile.Name, task.Branch, task.Issue, resolveTaskTitle(task.Issue, task.Issue), task.PRNumber)
	if err != nil {
		return fmt.Errorf("build pane metadata: %w", err)
	}
	if err := d.setPaneMetadata(ctx, pane.ID, metadata); err != nil {
		return fmt.Errorf("set pane metadata: %w", err)
	}
	startupSnapshot, err := d.agentHandshake(ctx, pane.ID, profile)
	if err != nil {
		return fmt.Errorf("agent handshake: %w", err)
	}
	worker.LastCapture = startupSnapshot.Output()

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

	if err := d.setIssueStatus(ctx, projectPath, task.Issue, IssueStateInProgress); err != nil {
		return fmt.Errorf("set issue status: %w", err)
	}

	if err := d.storeResumedTaskForProject(ctx, projectPath, task, worker, hasWorker, pane); err != nil {
		return err
	}

	spawned = false
	return nil
}

func (d *Daemon) storeResumedTask(ctx context.Context, task Task, worker Worker, hasWorker bool, pane Pane) error {
	return d.storeResumedTaskForProject(ctx, d.projectPathForTask(task), task, worker, hasWorker, pane)
}

func (d *Daemon) storeResumedTaskForProject(ctx context.Context, projectPath string, task Task, worker Worker, hasWorker bool, pane Pane) error {
	now := d.now()
	oldPaneID := task.PaneID

	task.Project = projectPath
	task.Status = TaskStatusActive
	if task.WorkerID == "" {
		task.WorkerID = worker.WorkerID
	}
	if task.WorkerID == "" {
		task.WorkerID = stableWorkerRef(task, worker)
	}
	if task.WorkerID == "" {
		task.WorkerID = strings.TrimSpace(pane.ID)
	}
	task.PaneID = pane.ID
	task.PaneName = workerPaneName(task.Issue, task.WorkerID)
	if task.PaneName == "" {
		task.PaneName = pane.Name
	}
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
			Project:      projectPath,
			WorkerID:     task.WorkerID,
			PaneName:     workerPaneName(task.Issue, task.WorkerID),
			Health:       WorkerHealthHealthy,
			AgentProfile: task.AgentProfile,
			ClonePath:    task.ClonePath,
			Issue:        task.Issue,
			RestartCount: worker.RestartCount,
			FirstCrashAt: worker.FirstCrashAt,
			CreatedAt:    now,
			LastSeenAt:   now,
		}
	}
	resumedWorker.Project = projectPath
	resumedWorker.WorkerID = task.WorkerID
	resumedWorker.PaneID = pane.ID
	resumedWorker.PaneName = workerPaneName(task.Issue, task.WorkerID)
	resumedWorker.Issue = task.Issue
	resumedWorker.ClonePath = task.ClonePath
	resumedWorker.AgentProfile = task.AgentProfile
	resumedWorker.Health = WorkerHealthHealthy
	resumedWorker.LastReviewCount = 0
	resumedWorker.LastInlineReviewCommentCount = 0
	resumedWorker.LastIssueCommentCount = 0
	resumedWorker.LastIssueCommentWatermark = ""
	resumedWorker.LastReviewUpdatedAt = time.Time{}
	resumedWorker.ReviewNudgeCount = 0
	resumedWorker.ReviewApproved = false
	resumedWorker.LastCIState = ""
	resumedWorker.CINudgeCount = 0
	resumedWorker.CIFailurePollCount = 0
	resumedWorker.CIEscalated = false
	resumedWorker.LastMergeableState = ""
	resumedWorker.NudgeCount = 0
	resumedWorker.LastCapture = worker.LastCapture
	resumedWorker.LastActivityAt = now
	resumedWorker.LastPRNumber = task.PRNumber
	if !hasWorker && task.PRNumber > 0 {
		resumedWorker.LastPushAt = now
	}
	resumedWorker.LastSeenAt = now
	resumedWorker.UpdatedAt = now
	if oldPaneID != "" && oldPaneID != pane.ID {
		if err := d.state.DeleteWorker(ctx, projectPath, oldPaneID); err != nil && !errors.Is(err, ErrWorkerNotFound) {
			return fmt.Errorf("delete worker after resume: %w", err)
		}
	}
	if err := d.state.PutWorker(ctx, resumedWorker); err != nil {
		return fmt.Errorf("store worker after resume: %w", err)
	}
	if err := d.state.PutTask(ctx, task); err != nil {
		return fmt.Errorf("store task after resume: %w", err)
	}
	d.ensureTaskMonitorForProject(projectPath, task.Issue)

	return nil
}
