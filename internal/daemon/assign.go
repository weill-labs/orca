package daemon

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var autonomousBacklogPromptPattern = regexp.MustCompile(strings.Join([]string{
	`(?i)\bpick up\b.*\b(?:work|issue|task|ticket)\b`,
	`\bfrom\b.*\b(?:backlog|queue)\b`,
	`\bnew work\b`,
	`\bnext\s+(?:issue|task|ticket)\b`,
	`\bfind\b.*\b(?:issue|task|work)\b.*\bbacklog\b`,
}, "|"))

func (d *Daemon) Assign(ctx context.Context, issue, prompt, agentProfile string, title ...string) error {
	return d.assign(ctx, d.project, issue, prompt, agentProfile, "", title...)
}

func (d *Daemon) AssignWithCallerPane(ctx context.Context, issue, prompt, agentProfile, callerPane string, title ...string) error {
	return d.assign(ctx, d.project, issue, prompt, agentProfile, callerPane, title...)
}

func (d *Daemon) assign(ctx context.Context, projectPath, issue, prompt, agentProfile, callerPane string, title ...string) error {
	if err := d.requireStarted(); err != nil {
		return err
	}

	profile, err := d.config.AgentProfile(ctx, agentProfile)
	if err != nil {
		return fmt.Errorf("load agent profile %q: %w", agentProfile, err)
	}
	if profile.Name == "" {
		profile.Name = agentProfile
	}
	profile = enforceLifecycleProfile(profile)

	if err := d.validateAssignment(ctx, projectPath, issue, prompt); err != nil {
		return err
	}
	prompt = wrapAssignmentPrompt(profile, prompt)

	assignmentBranch := issue
	prNumber, err := d.lookupOpenPRNumber(ctx, projectPath, assignmentBranch)
	if err != nil {
		return fmt.Errorf("check open PRs for %s: %w", issue, err)
	}
	adoptingOpenPR := prNumber > 0

	now := d.now()
	claimedTask := Task{
		Project:      projectPath,
		Issue:        issue,
		Status:       TaskStatusStarting,
		Prompt:       prompt,
		CallerPane:   strings.TrimSpace(callerPane),
		Branch:       assignmentBranch,
		AgentProfile: profile.Name,
		PRNumber:     prNumber,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	previousTask, err := d.state.ClaimTask(ctx, claimedTask)
	if err != nil {
		return err
	}
	restoreReservation := func() {
		_ = d.state.RestoreTask(context.WithoutCancel(ctx), projectPath, issue, previousTask)
	}

	claimedWorker, err := d.state.ClaimWorker(ctx, Worker{
		Project:      projectPath,
		Issue:        issue,
		AgentProfile: profile.Name,
		Health:       WorkerHealthHealthy,
		CreatedAt:    now,
		LastSeenAt:   now,
		UpdatedAt:    now,
	})
	if err != nil {
		restoreReservation()
		return fmt.Errorf("claim worker: %w", err)
	}
	restoreWorkerClaim := func() {
		_ = d.releaseWorkerClaim(context.WithoutCancel(ctx), claimedWorker)
	}

	clone, err := d.pool.Acquire(ctx, projectPath, issue)
	if err != nil {
		restoreWorkerClaim()
		restoreReservation()
		return fmt.Errorf("acquire clone: %w", err)
	}

	prepareClone := d.prepareClone
	if adoptingOpenPR {
		prepareClone = d.prepareAdoptedClone
	}
	if err := prepareClone(ctx, clone.Path, assignmentBranch, claimedWorker.WorkerID); err != nil {
		_ = d.pool.Release(ctx, projectPath, clone)
		restoreWorkerClaim()
		restoreReservation()
		return fmt.Errorf("prepare clone: %w", err)
	}
	clone.CurrentBranch = assignmentBranch
	clone.AssignedTask = issue

	pane, err := d.spawnWorkerPane(ctx, claimedTask, claimedWorker.WorkerID, clone.Path, profile)
	if err != nil {
		_ = d.cleanupCloneAndReleaseForProject(ctx, projectPath, clone, issue)
		restoreWorkerClaim()
		restoreReservation()
		return fmt.Errorf("spawn pane: %w", err)
	}

	metadata, err := d.assignmentPaneMetadata(ctx, projectPath, pane.ID, profile.Name, assignmentBranch, issue, d.resolveAssignmentTitle(ctx, issue, firstTitle(title)), prNumber)
	if err != nil {
		_ = d.rollbackAssignmentForProject(ctx, projectPath, clone, pane, issue)
		restoreWorkerClaim()
		restoreReservation()
		return fmt.Errorf("build pane metadata: %w", err)
	}

	if err := d.setPaneMetadata(ctx, pane.ID, metadata); err != nil {
		_ = d.rollbackAssignmentForProject(ctx, projectPath, clone, pane, issue)
		restoreWorkerClaim()
		restoreReservation()
		return fmt.Errorf("set pane metadata: %w", err)
	}

	task := claimedTask
	task.WorkerID = claimedWorker.WorkerID
	task.PaneID = pane.ID
	task.PaneName = claimedWorker.WorkerID
	task.CloneName = clone.Name
	task.ClonePath = clone.Path
	task.UpdatedAt = d.now()
	worker := Worker{
		Project:               projectPath,
		WorkerID:              claimedWorker.WorkerID,
		PaneID:                pane.ID,
		PaneName:              claimedWorker.WorkerID,
		Issue:                 issue,
		ClonePath:             clone.Path,
		AgentProfile:          profile.Name,
		Health:                WorkerHealthHealthy,
		LastReviewCount:       0,
		LastIssueCommentCount: 0,
		ReviewNudgeCount:      0,
		LastCIState:           "",
		CINudgeCount:          0,
		CIFailurePollCount:    0,
		CIEscalated:           false,
		LastMergeableState:    "",
		NudgeCount:            0,
		LastCapture:           "",
		CreatedAt:             claimedWorker.CreatedAt,
		LastActivityAt:        now,
		LastSeenAt:            now,
		UpdatedAt:             now,
	}
	if err := d.state.PutTask(ctx, task); err != nil {
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, err, restoreReservation)
		return fmt.Errorf("store pending task: %w", err)
	}
	if err := d.state.PutWorker(ctx, worker); err != nil {
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, err, restoreReservation)
		return fmt.Errorf("store pending worker: %w", err)
	}

	if err := d.agentHandshake(ctx, pane.ID, profile); err != nil {
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, err, restoreReservation)
		return fmt.Errorf("agent handshake: %w", err)
	}

	if err := d.amux.SendKeys(ctx, pane.ID, prompt); err != nil {
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, err, restoreReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.amux.WaitIdleSettle(ctx, pane.ID, defaultAgentHandshakeTimeout, defaultPromptSettleDuration); err != nil {
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, err, restoreReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.amux.SendKeys(ctx, pane.ID, "Enter"); err != nil {
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, err, restoreReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.confirmPromptDelivery(ctx, pane.ID, profile); err != nil {
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, err, restoreReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.setIssueStatus(ctx, projectPath, issue, IssueStateInProgress); err != nil {
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, err, restoreReservation)
		return fmt.Errorf("set issue status: %w", err)
	}

	task.Status = TaskStatusActive
	task.UpdatedAt = d.now()
	if err := d.state.PutTask(ctx, task); err != nil {
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, err, restoreReservation)
		return fmt.Errorf("store task: %w", err)
	}
	d.ensureTaskMonitorForProject(projectPath, issue)

	d.emit(ctx, Event{
		Time:         now,
		Type:         EventTaskAssigned,
		Project:      projectPath,
		Issue:        issue,
		WorkerID:     claimedWorker.WorkerID,
		PaneID:       pane.ID,
		PaneName:     claimedWorker.WorkerID,
		CloneName:    clone.Name,
		ClonePath:    clone.Path,
		Branch:       assignmentBranch,
		AgentProfile: profile.Name,
		PRNumber:     prNumber,
		Message:      "task assigned",
	})
	return nil
}

func (d *Daemon) validateAssignment(ctx context.Context, projectPath, issue, prompt string) error {
	if err := validateAssignmentPrompt(prompt); err != nil {
		return err
	}

	existingTask, err := d.state.TaskByIssue(ctx, projectPath, issue)
	if err == nil {
		if taskBlocksAssignment(existingTask.Status) {
			return fmt.Errorf("issue %s already assigned", issue)
		}
	} else if !errors.Is(err, ErrTaskNotFound) {
		return fmt.Errorf("load task %s: %w", issue, err)
	}

	return nil
}

func validateAssignmentPrompt(prompt string) error {
	if autonomousBacklogPromptPattern.MatchString(prompt) {
		return errors.New("assignment prompt cannot ask the worker to pick backlog work autonomously; assign a specific issue instead")
	}
	return nil
}

func (d *Daemon) failPendingAssignment(ctx context.Context, projectPath, issue string, clone Clone, pane Pane, worker Worker, profile AgentProfile, err error, releaseReservation func()) {
	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventTaskAssignFailed,
		Project:      projectPath,
		Issue:        issue,
		WorkerID:     worker.WorkerID,
		PaneID:       pane.ID,
		CloneName:    clone.Name,
		ClonePath:    clone.Path,
		Branch:       issue,
		AgentProfile: profile.Name,
		Message:      err.Error(),
	})
	_ = d.rollbackAssignmentForProject(ctx, projectPath, clone, pane, issue)
	if releaseErr := d.releaseWorkerClaim(context.WithoutCancel(ctx), worker); releaseErr != nil && !errors.Is(releaseErr, ErrWorkerNotFound) {
		_ = releaseErr
	}
	releaseReservation()
}
