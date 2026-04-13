package daemon

import (
	"context"
	"fmt"
	"strings"
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

func (d *Daemon) checkTaskPRPoll(ctx context.Context, active ActiveAssignment) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}
	now := d.now()
	if syncWorkerPRTracking(now, &update.Active) {
		update.WorkerChanged = true
	}
	update.Active.Worker.LastPRPollAt = now
	update.WorkerChanged = true
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return update
	}

	if update.Active.Task.PRNumber == 0 {
		prNumber, err := d.lookupPRNumber(ctx, update.Active.Task.Project, update.Active.Task.Branch)
		if err != nil {
			d.appendGitHubRateLimitEvent(&update, profile, err)
			return update
		}
		if prNumber > 0 {
			metadata, err := d.prPaneMetadata(ctx, update.Active, prNumber)
			if err != nil {
				return update
			}
			update.PaneMetadata = mergeMetadata(update.PaneMetadata, metadata)
			update.Active.Worker.LastPRNumber = prNumber
			update.Active.Worker.LastPushAt = now
			update.Active.Task.PRNumber = prNumber
			update.Active.Task.UpdatedAt = now
			update.TaskChanged = true
			update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventPRDetected, "pull request detected"))
		}
	}

	if update.Active.Task.PRNumber == 0 {
		return update
	}
	if entry, err := d.state.MergeEntry(ctx, update.Active.Task.Project, update.Active.Task.PRNumber); err == nil && entry != nil {
		merged, err := d.isPRMerged(ctx, update.Active.Task.Project, update.Active.Task.PRNumber)
		if err != nil {
			d.appendGitHubRateLimitEvent(&update, profile, err)
			return update
		}
		if merged {
			update.PRMerged = true
		}
		return update
	}

	d.handlePRChecksPoll(ctx, &update, profile)

	merged, err := d.isPRMerged(ctx, update.Active.Task.Project, update.Active.Task.PRNumber)
	if err != nil {
		if d.appendGitHubRateLimitEvent(&update, profile, err) {
			return update
		}
		return d.continuePRFollowUpPolls(ctx, update, profile)
	}
	if !merged {
		return d.continuePRFollowUpPolls(ctx, update, profile)
	}

	update.PRMerged = true
	return update
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

func (d *Daemon) lookupPRNumber(ctx context.Context, projectPath, branch string) (int, error) {
	return d.gitHubClientForContext(ctx, projectPath).lookupPRNumber(ctx, branch)
}

func (d *Daemon) lookupOpenPRNumber(ctx context.Context, projectPath, branch string) (int, error) {
	return d.gitHubClientForContext(ctx, projectPath).lookupOpenPRNumber(ctx, branch)
}

func (d *Daemon) isPRMerged(ctx context.Context, projectPath string, prNumber int) (bool, error) {
	return d.gitHubClientForContext(ctx, projectPath).isPRMerged(ctx, prNumber)
}
