package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (d *Daemon) Enqueue(ctx context.Context, prNumber int) (MergeQueueActionResult, error) {
	return d.enqueue(ctx, d.project, prNumber)
}

func (d *Daemon) enqueue(ctx context.Context, projectPath string, prNumber int) (MergeQueueActionResult, error) {
	if err := d.requireStarted(); err != nil {
		return MergeQueueActionResult{}, err
	}

	active, err := d.state.ActiveAssignmentByPRNumber(ctx, projectPath, prNumber)
	if err != nil {
		return MergeQueueActionResult{}, fmt.Errorf("PR #%d is not associated with an active assignment", prNumber)
	}

	now := d.now()
	position, err := d.state.EnqueueMerge(ctx, MergeQueueEntry{
		Project:   projectPath,
		Issue:     active.Task.Issue,
		PRNumber:  prNumber,
		Status:    MergeQueueStatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		lowered := strings.ToLower(err.Error())
		if strings.Contains(lowered, "queued") || strings.Contains(lowered, "unique") {
			return MergeQueueActionResult{}, fmt.Errorf("PR #%d is already queued for landing", prNumber)
		}
		return MergeQueueActionResult{}, err
	}

	d.emit(ctx, d.mergeQueueEvent(&active, EventPREnqueued, prNumber, "pull request queued for landing", now))

	return MergeQueueActionResult{
		Project:   projectPath,
		PRNumber:  prNumber,
		Status:    "queued",
		Position:  position,
		UpdatedAt: now,
	}, nil
}

func (d *Daemon) checkTaskPRPoll(ctx context.Context, active ActiveAssignment) (update TaskStateUpdate) {
	update = TaskStateUpdate{Active: active}
	update.Active.Task.State = normalizeTaskState(update.Active.Task)
	now := d.now()
	if syncWorkerPRTracking(now, &update.Active) {
		update.WorkerChanged = true
	}
	update.Active.Worker.LastPRPollAt = now
	update.WorkerChanged = true
	profile := AgentProfile{Name: active.Task.AgentProfile}
	traceAction := "poll_complete"
	var traceErr error
	defer func() {
		d.tracePRPoll(&update, profile, traceAction, traceErr)
	}()

	loadedProfile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		traceAction = "profile_error"
		traceErr = err
		return update
	}
	profile = loadedProfile

	switch update.Active.Task.State {
	case TaskStateDone:
		traceAction = "task_done"
		return update
	case TaskStateMerged:
		update.PRMerged = true
		update.CompletionStatus = TaskStatusDone
		update.CompletionEventType = EventTaskCompleted
		update.CompletionMerged = true
		update.CompletionMessage = "task finished"
		traceAction = "task_already_merged"
		return update
	}

	if update.Active.Task.PRNumber == 0 {
		prNumber, err := d.lookupPRNumber(ctx, update.Active.Task.Project, update.Active.Task.Branch)
		if err != nil {
			traceAction = "lookup_pr_error"
			traceErr = err
			d.appendGitHubRateLimitEvent(&update, profile, err)
			return update
		}
		if prNumber == 0 {
			discoveredBranch := ""
			prNumber, discoveredBranch, err = d.findPRByIssueID(ctx, update.Active.Task.Project, update.Active.Task.Issue)
			if err != nil {
				traceAction = "find_pr_by_issue_error"
				traceErr = err
				d.appendGitHubRateLimitEvent(&update, profile, err)
				return update
			}
			if prNumber > 0 {
				d.recordDiscoveredBranch(&update, discoveredBranch, now)
			}
		}
		if prNumber > 0 {
			metadata, err := d.prPaneMetadata(ctx, update.Active, prNumber)
			if err != nil {
				traceAction = "pr_metadata_error"
				traceErr = err
				return update
			}
			update.PaneMetadata = mergeMetadata(update.PaneMetadata, metadata)
			update.Active.Worker.LastPRNumber = prNumber
			update.Active.Worker.LastPushAt = now
			update.Active.Task.PRNumber = prNumber
			if setTaskState(&update.Active.Task, TaskStatePRDetected, now) {
				update.TaskChanged = true
			}
			update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventPRDetected, "pull request detected"))
			traceAction = "pr_detected"
		}
	}

	if update.Active.Task.PRNumber == 0 {
		traceAction = "no_pr_found"
		return update
	}
	if entry, err := d.state.MergeEntry(ctx, update.Active.Task.Project, update.Active.Task.PRNumber); err == nil && entry != nil {
		_, err := d.resolvePRTerminalState(ctx, &update, profile, now)
		if err != nil {
			traceAction = "queued_terminal_state_error"
			traceErr = err
			d.appendGitHubRateLimitEvent(&update, profile, err)
			return update
		}
		traceAction = "merge_queue_terminal_state_polled"
		return update
	}

	handled, err := d.resolvePRTerminalState(ctx, &update, profile, now)
	if err != nil {
		traceAction = "terminal_state_error"
		traceErr = err
		if d.appendGitHubRateLimitEvent(&update, profile, err) {
			return update
		}
		if d.handlePRChecksPoll(ctx, &update, profile) {
			traceAction = "ci_poll_after_terminal_error"
			return update
		}
		traceAction = "follow_up_after_terminal_error"
		return d.continuePRFollowUpPolls(ctx, update, profile)
	}
	if handled {
		traceAction = "terminal_state_handled"
		return update
	}

	if d.handlePRChecksPoll(ctx, &update, profile) {
		traceAction = "ci_poll_handled"
		return update
	}
	traceAction = "follow_up_poll"
	return d.continuePRFollowUpPolls(ctx, update, profile)
}

func (d *Daemon) continuePRFollowUpPolls(ctx context.Context, update TaskStateUpdate, profile AgentProfile) TaskStateUpdate {
	d.handlePRMergeablePoll(ctx, &update, profile)
	reviewUpdate := d.checkTaskReviewPoll(ctx, update.Active, profile)
	return mergeTaskStateUpdates(update, reviewUpdate)
}

func mergeTaskStateUpdates(base, next TaskStateUpdate) TaskStateUpdate {
	merged := base
	merged.Active = mergeActiveAssignment(base, next)
	merged.TaskChanged = merged.TaskChanged || next.TaskChanged
	merged.WorkerChanged = merged.WorkerChanged || next.WorkerChanged
	merged.PaneMetadata = mergeMetadata(merged.PaneMetadata, next.PaneMetadata)
	merged.Events = append(merged.Events, next.Events...)
	merged.PRMerged = merged.PRMerged || next.PRMerged
	if next.CompletionStatus != "" {
		merged.CompletionStatus = next.CompletionStatus
		merged.CompletionEventType = next.CompletionEventType
		merged.CompletionMerged = next.CompletionMerged
		merged.CompletionWrapUpPrompt = next.CompletionWrapUpPrompt
		merged.CompletionMessage = next.CompletionMessage
	}
	merged.nudges = append(merged.nudges, next.nudges...)
	return merged
}

func mergeActiveAssignment(base, next TaskStateUpdate) ActiveAssignment {
	merged := next.Active
	if base.TaskChanged && !next.TaskChanged {
		merged.Task = base.Active.Task
	}
	if base.WorkerChanged && !next.WorkerChanged {
		merged.Worker = base.Active.Worker
	}
	return merged
}

func markTaskPRMerged(update *TaskStateUpdate, now time.Time) {
	if update == nil {
		return
	}
	if setTaskState(&update.Active.Task, TaskStateMerged, now) {
		update.TaskChanged = true
	}
	update.PRMerged = true
}

func (d *Daemon) resolvePRTerminalState(ctx context.Context, update *TaskStateUpdate, profile AgentProfile, now time.Time) (bool, error) {
	if update == nil {
		return false, nil
	}

	state, err := d.lookupPRTerminalState(ctx, update.Active.Task.Project, update.Active.Task.PRNumber)
	if err != nil {
		return false, err
	}
	return d.applyPRTerminalState(update, profile, state, now), nil
}

func (d *Daemon) applyPRTerminalState(update *TaskStateUpdate, profile AgentProfile, state prTerminalState, now time.Time) bool {
	if update == nil {
		return false
	}

	switch {
	case state.closedWithoutMerge:
		if setTaskState(&update.Active.Task, TaskStateDone, now) {
			update.TaskChanged = true
		}
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventPRClosed, "pull request closed without merging"))
		update.CompletionStatus = TaskStatusFailed
		update.CompletionEventType = EventTaskFailed
		update.CompletionWrapUpPrompt = closedWrapUpPrompt
		update.CompletionMessage = "pull request closed without merging"
		return true
	case state.merged:
		markTaskPRMerged(update, now)
		return true
	default:
		return false
	}
}

func (d *Daemon) lookupPRNumber(ctx context.Context, projectPath, branch string) (int, error) {
	return d.gitHubClientForContext(ctx, projectPath).lookupPRNumber(ctx, branch)
}

func (d *Daemon) findPRByIssueID(ctx context.Context, projectPath, issueID string) (int, string, error) {
	return d.gitHubClientForContext(ctx, projectPath).findPRByIssueID(ctx, issueID)
}

func (d *Daemon) lookupOpenPRNumber(ctx context.Context, projectPath, branch string) (int, error) {
	return d.gitHubClientForContext(ctx, projectPath).lookupOpenPRNumber(ctx, branch)
}

func (d *Daemon) lookupOpenOrMergedPRNumber(ctx context.Context, projectPath, branch string) (int, bool, error) {
	return d.gitHubClientForContext(ctx, projectPath).lookupOpenOrMergedPRNumber(ctx, branch)
}

func (d *Daemon) lookupPRTerminalState(ctx context.Context, projectPath string, prNumber int) (prTerminalState, error) {
	return d.gitHubClientForContext(ctx, projectPath).lookupPRTerminalState(ctx, prNumber)
}

func (d *Daemon) isPRMerged(ctx context.Context, projectPath string, prNumber int) (bool, error) {
	return d.gitHubClientForContext(ctx, projectPath).isPRMerged(ctx, prNumber)
}

func (d *Daemon) recordDiscoveredBranch(update *TaskStateUpdate, branch string, now time.Time) {
	if update == nil {
		return
	}

	branch = strings.TrimSpace(branch)
	if branch == "" || branch == update.Active.Task.Branch {
		return
	}

	update.Active.Task.Branch = branch
	update.Active.Task.UpdatedAt = now
	update.TaskChanged = true
	update.PaneMetadata = mergeMetadata(update.PaneMetadata, map[string]string{
		"branch": branch,
	})
}
