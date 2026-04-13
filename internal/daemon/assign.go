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

var autonomousBacklogPromptOverridePattern = regexp.MustCompile(strings.Join([]string{
	`(?i)\bnew work\b`,
	`\bnext\s+(?:issue|task|ticket)\b`,
	`\banother\s+(?:issue|task|ticket)\b`,
}, "|"))

var explicitIssueIDPattern = regexp.MustCompile(`\b[A-Za-z][A-Za-z0-9_]*-\d+\b`)

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
		err = fmt.Errorf("load agent profile %q: %w", agentProfile, err)
		d.emitAssignFailure(ctx, projectPath, issue, "", strings.TrimSpace(agentProfile), Clone{}, Pane{}, 0, err)
		return err
	}
	if profile.Name == "" {
		profile.Name = agentProfile
	}
	profile = enforceLifecycleProfile(profile)

	if err := d.validateAssignment(ctx, projectPath, issue, prompt); err != nil {
		d.emitAssignFailure(ctx, projectPath, issue, "", profile.Name, Clone{}, Pane{}, 0, err)
		return err
	}
	prompt = wrapAssignmentPrompt(profile, prompt)

	assignmentBranch := issue
	prNumber, err := d.lookupOpenPRNumber(ctx, projectPath, assignmentBranch)
	if err != nil {
		err = fmt.Errorf("check open PRs for %s: %w", issue, err)
		d.emitAssignFailure(ctx, projectPath, issue, "", profile.Name, Clone{}, Pane{}, 0, err)
		return err
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
		d.emitAssignFailure(ctx, projectPath, issue, "", profile.Name, Clone{}, Pane{}, prNumber, err)
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
		err = fmt.Errorf("claim worker: %w", err)
		d.emitAssignFailure(ctx, projectPath, issue, "", profile.Name, Clone{}, Pane{}, prNumber, err)
		return err
	}
	restoreWorkerClaim := func() {
		_ = d.releaseWorkerClaim(context.WithoutCancel(ctx), claimedWorker)
	}
	if err := d.clearStaleWorkerPaneRef(ctx, &claimedWorker); err != nil {
		restoreWorkerClaim()
		restoreReservation()
		err = fmt.Errorf("clear stale worker pane ref: %w", err)
		d.emitAssignFailure(ctx, projectPath, issue, claimedWorker.WorkerID, profile.Name, Clone{}, Pane{}, prNumber, err)
		return err
	}

	clone, err := d.pool.Acquire(ctx, projectPath, issue)
	if err != nil {
		restoreWorkerClaim()
		restoreReservation()
		err = fmt.Errorf("acquire clone: %w", err)
		d.emitAssignFailure(ctx, projectPath, issue, claimedWorker.WorkerID, profile.Name, Clone{}, Pane{}, prNumber, err)
		return err
	}

	prepareClone := d.prepareClone
	if adoptingOpenPR {
		prepareClone = d.prepareAdoptedClone
	}
	if err := prepareClone(ctx, clone.Path, assignmentBranch, claimedWorker.WorkerID); err != nil {
		_ = d.pool.Release(ctx, projectPath, clone)
		restoreWorkerClaim()
		restoreReservation()
		err = fmt.Errorf("prepare clone: %w", err)
		d.emitAssignFailure(ctx, projectPath, issue, claimedWorker.WorkerID, profile.Name, clone, Pane{}, prNumber, err)
		return err
	}
	clone.CurrentBranch = assignmentBranch
	clone.AssignedTask = issue

	pane, err := d.spawnWorkerPane(ctx, claimedTask, claimedWorker.WorkerID, clone.Path, profile)
	if err != nil {
		_ = d.cleanupCloneAndReleaseForProject(ctx, projectPath, clone, issue)
		restoreWorkerClaim()
		restoreReservation()
		err = fmt.Errorf("spawn pane: %w", err)
		d.emitAssignFailure(ctx, projectPath, issue, claimedWorker.WorkerID, profile.Name, clone, Pane{}, prNumber, err)
		return err
	}
	paneSpawnPendingCleanup := true
	defer func() {
		if !paneSpawnPendingCleanup {
			return
		}
		_ = ignorePaneAlreadyGoneError(d.amux.KillPane(context.WithoutCancel(ctx), paneKillRef(pane)))
	}()
	var worker Worker
	rollbackSpawnedPane := func() {
		paneSpawnPendingCleanup = false
		_ = d.rollbackAssignmentForProject(ctx, projectPath, clone, pane, issue)
	}
	failSpawnedAssignment := func(err error) {
		paneSpawnPendingCleanup = false
		d.failPendingAssignment(ctx, projectPath, issue, clone, pane, worker, profile, prNumber, err, restoreReservation)
	}

	metadata, err := d.assignmentPaneMetadata(ctx, projectPath, pane.ID, profile.Name, assignmentBranch, issue, d.resolveAssignmentTitle(ctx, issue, firstTitle(title)), prNumber)
	if err != nil {
		rollbackSpawnedPane()
		restoreWorkerClaim()
		restoreReservation()
		err = fmt.Errorf("build pane metadata: %w", err)
		d.emitAssignFailure(ctx, projectPath, issue, claimedWorker.WorkerID, profile.Name, clone, pane, prNumber, err)
		return err
	}

	if err := d.setPaneMetadata(ctx, pane.ID, metadata); err != nil {
		rollbackSpawnedPane()
		restoreWorkerClaim()
		restoreReservation()
		err = fmt.Errorf("set pane metadata: %w", err)
		d.emitAssignFailure(ctx, projectPath, issue, claimedWorker.WorkerID, profile.Name, clone, pane, prNumber, err)
		return err
	}

	task := claimedTask
	task.WorkerID = claimedWorker.WorkerID
	task.PaneID = pane.ID
	task.PaneName = workerPaneName(issue, claimedWorker.WorkerID)
	task.CloneName = clone.Name
	task.ClonePath = clone.Path
	task.UpdatedAt = d.now()
	worker = Worker{
		Project:                      projectPath,
		WorkerID:                     claimedWorker.WorkerID,
		PaneID:                       pane.ID,
		PaneName:                     workerPaneName(issue, claimedWorker.WorkerID),
		Issue:                        issue,
		ClonePath:                    clone.Path,
		AgentProfile:                 profile.Name,
		Health:                       WorkerHealthHealthy,
		LastReviewCount:              0,
		LastInlineReviewCommentCount: 0,
		LastIssueCommentCount:        0,
		ReviewNudgeCount:             0,
		LastCIState:                  "",
		CINudgeCount:                 0,
		CIFailurePollCount:           0,
		CIEscalated:                  false,
		LastMergeableState:           "",
		NudgeCount:                   0,
		LastCapture:                  "",
		CreatedAt:                    claimedWorker.CreatedAt,
		LastActivityAt:               now,
		LastSeenAt:                   now,
		UpdatedAt:                    now,
	}
	if err := d.state.PutTask(ctx, task); err != nil {
		failSpawnedAssignment(err)
		return fmt.Errorf("store pending task: %w", err)
	}
	if err := d.state.PutWorker(ctx, worker); err != nil {
		failSpawnedAssignment(err)
		return fmt.Errorf("store pending worker: %w", err)
	}

	if err := d.agentHandshake(ctx, pane.ID, profile); err != nil {
		failSpawnedAssignment(err)
		return fmt.Errorf("agent handshake: %w", err)
	}

	if err := d.sendPromptAndEnter(ctx, pane.ID, prompt); err != nil {
		failSpawnedAssignment(err)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.confirmPromptDelivery(ctx, pane.ID, profile); err != nil {
		failSpawnedAssignment(err)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.setIssueStatus(ctx, projectPath, issue, IssueStateInProgress); err != nil {
		failSpawnedAssignment(err)
		return fmt.Errorf("set issue status: %w", err)
	}

	task.Status = TaskStatusActive
	task.UpdatedAt = d.now()
	if err := d.state.PutTask(ctx, task); err != nil {
		failSpawnedAssignment(err)
		return fmt.Errorf("store task: %w", err)
	}
	d.ensureTaskMonitorForProject(projectPath, issue)
	paneSpawnPendingCleanup = false

	d.emit(ctx, Event{
		Time:         now,
		Type:         EventTaskAssigned,
		Project:      projectPath,
		Issue:        issue,
		WorkerID:     claimedWorker.WorkerID,
		PaneID:       pane.ID,
		PaneName:     workerPaneName(issue, claimedWorker.WorkerID),
		CloneName:    clone.Name,
		ClonePath:    clone.Path,
		Branch:       assignmentBranch,
		AgentProfile: profile.Name,
		PRNumber:     prNumber,
		Message:      taskAssignedMessage(pane),
	})
	return nil
}

func taskAssignedMessage(pane Pane) string {
	if window := strings.TrimSpace(pane.Window); window != "" {
		return fmt.Sprintf("task assigned in fallback amux window %s after target window ran out of split space", window)
	}
	return "task assigned"
}

func (d *Daemon) validateAssignment(ctx context.Context, projectPath, issue, prompt string) error {
	if err := validateAssignmentPrompt(issue, prompt); err != nil {
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

func (d *Daemon) emitAssignFailure(ctx context.Context, projectPath, issue, workerID, profileName string, clone Clone, pane Pane, prNumber int, err error) {
	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventTaskAssignFailed,
		Project:      projectPath,
		Issue:        issue,
		WorkerID:     workerID,
		PaneID:       pane.ID,
		PaneName:     pane.Name,
		CloneName:    clone.Name,
		ClonePath:    clone.Path,
		Branch:       issue,
		AgentProfile: profileName,
		PRNumber:     prNumber,
		Message:      err.Error(),
	})
}

func validateAssignmentPrompt(issue, prompt string) error {
	if !autonomousBacklogPromptPattern.MatchString(prompt) {
		return nil
	}
	if autonomousBacklogPromptOverridePattern.MatchString(prompt) {
		return errors.New("assignment prompt cannot ask the worker to pick backlog work autonomously; assign a specific issue instead")
	}
	if !promptMentionsAssignedIssue(prompt, issue) || promptMentionsDifferentIssue(prompt, issue) {
		return errors.New("assignment prompt cannot ask the worker to pick backlog work autonomously; assign a specific issue instead")
	}
	return nil
}

func promptMentionsAssignedIssue(prompt, issue string) bool {
	issue = strings.TrimSpace(issue)
	if issue == "" {
		return false
	}
	issuePattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(issue) + `\b`)
	return issuePattern.MatchString(prompt)
}

func promptMentionsDifferentIssue(prompt, issue string) bool {
	issue = strings.TrimSpace(issue)
	if issue == "" {
		return false
	}
	for _, candidate := range explicitIssueIDPattern.FindAllString(prompt, -1) {
		if !strings.EqualFold(candidate, issue) {
			return true
		}
	}
	return false
}

func (d *Daemon) failPendingAssignment(ctx context.Context, projectPath, issue string, clone Clone, pane Pane, worker Worker, profile AgentProfile, prNumber int, err error, releaseReservation func()) {
	d.emitAssignFailure(ctx, projectPath, issue, worker.WorkerID, profile.Name, clone, pane, prNumber, err)
	_ = d.rollbackAssignmentForProject(ctx, projectPath, clone, pane, issue)
	if releaseErr := d.releaseWorkerClaim(context.WithoutCancel(ctx), worker); releaseErr != nil && !errors.Is(releaseErr, ErrWorkerNotFound) {
		_ = releaseErr
	}
	releaseReservation()
}
