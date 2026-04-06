package daemon

import (
	"context"
	"fmt"
	"strings"
)

func (d *Daemon) Enqueue(ctx context.Context, prNumber int) (MergeQueueActionResult, error) {
	if err := d.requireStarted(); err != nil {
		return MergeQueueActionResult{}, err
	}

	active, err := d.state.ActiveAssignmentByPRNumber(ctx, d.project, prNumber)
	if err != nil {
		return MergeQueueActionResult{}, fmt.Errorf("PR #%d is not associated with an active assignment", prNumber)
	}

	now := d.now()
	position, err := d.state.EnqueueMerge(ctx, MergeQueueEntry{
		Project:   d.project,
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
		Project:   d.project,
		PRNumber:  prNumber,
		Status:    "queued",
		Position:  position,
		UpdatedAt: now,
	}, nil
}

func (d *Daemon) processMergeQueue(ctx context.Context) {
	entry, err := d.state.NextMergeEntry(ctx, d.project)
	if err != nil || entry == nil {
		return
	}

	active, err := d.state.ActiveAssignmentByPRNumber(ctx, d.project, entry.PRNumber)
	if err != nil {
		d.emit(ctx, d.mergeQueueEvent(nil, EventPRLandingFailed, entry.PRNumber, fmt.Sprintf("PR #%d is no longer tracked by an active assignment", entry.PRNumber), d.now()))
		_ = d.state.DeleteMergeEntry(ctx, d.project, entry.PRNumber)
		return
	}

	switch entry.Status {
	case "", MergeQueueStatusQueued:
		d.emit(ctx, d.mergeQueueEvent(&active, EventPRLandingStarted, entry.PRNumber, "processing queued PR landing", d.now()))
		if err := d.rebaseQueuedPR(ctx, entry.PRNumber); err != nil {
			d.handleQueuedPRFailure(ctx, active, entry.PRNumber, mergeQueueRebaseConflictPrompt(entry.PRNumber), err)
			_ = d.state.DeleteMergeEntry(ctx, d.project, entry.PRNumber)
			return
		}
		entry.Status = MergeQueueStatusAwaitingChecks
		entry.UpdatedAt = d.now()
		_ = d.state.UpdateMergeEntry(ctx, *entry)
	case MergeQueueStatusAwaitingChecks:
		ciState, err := d.lookupPRChecksState(ctx, entry.PRNumber)
		if err != nil {
			return
		}
		switch ciState {
		case ciStatePass, ciStateSkipping:
			if err := d.mergeQueuedPR(ctx, entry.PRNumber); err != nil {
				d.handleQueuedPRFailure(ctx, active, entry.PRNumber, mergeQueueMergeFailedPrompt(entry.PRNumber), err)
				_ = d.state.DeleteMergeEntry(ctx, d.project, entry.PRNumber)
				return
			}
			_ = d.state.DeleteMergeEntry(ctx, d.project, entry.PRNumber)
		case ciStateFail, ciStateCancel:
			d.handleQueuedPRFailure(ctx, active, entry.PRNumber, mergeQueueChecksFailedPrompt(entry.PRNumber), fmt.Errorf("required checks state is %s", ciState))
			_ = d.state.DeleteMergeEntry(ctx, d.project, entry.PRNumber)
		}
	default:
		entry.Status = MergeQueueStatusQueued
		entry.UpdatedAt = d.now()
		_ = d.state.UpdateMergeEntry(ctx, *entry)
	}
}

func (d *Daemon) checkTaskPRPoll(ctx context.Context, active ActiveAssignment) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}

	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return update
	}

	if update.Active.Task.PRNumber == 0 {
		prNumber, err := d.lookupPRNumber(ctx, update.Active.Task.Branch)
		if err != nil {
			return update
		}
		if prNumber > 0 {
			metadata, err := d.prPaneMetadata(ctx, update.Active, prNumber)
			if err != nil {
				return update
			}
			update.PaneMetadata = mergeMetadata(update.PaneMetadata, metadata)
			update.Active.Task.PRNumber = prNumber
			update.Active.Task.UpdatedAt = d.now()
			update.TaskChanged = true
			update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventPRDetected, "pull request detected"))
		}
	}

	if update.Active.Task.PRNumber == 0 {
		return update
	}
	if entry, err := d.state.MergeEntry(ctx, d.project, update.Active.Task.PRNumber); err == nil && entry != nil {
		return update
	}

	d.handlePRChecksPoll(ctx, &update, profile)

	merged, err := d.isPRMerged(ctx, update.Active.Task.PRNumber)
	if err != nil || !merged {
		d.handlePRMergeablePoll(ctx, &update, profile)
		reviewUpdate := d.checkTaskReviewPoll(ctx, update.Active, profile)
		update = mergeTaskStateUpdates(update, reviewUpdate)
		return update
	}

	update.PRMerged = true
	return update
}

func mergeTaskStateUpdates(base, next TaskStateUpdate) TaskStateUpdate {
	merged := base
	merged.Active = next.Active
	merged.TaskChanged = merged.TaskChanged || next.TaskChanged
	merged.WorkerChanged = merged.WorkerChanged || next.WorkerChanged
	merged.PaneMetadata = mergeMetadata(merged.PaneMetadata, next.PaneMetadata)
	merged.Events = append(merged.Events, next.Events...)
	merged.PRMerged = merged.PRMerged || next.PRMerged
	return merged
}

func (d *Daemon) rebaseQueuedPR(ctx context.Context, prNumber int) error {
	_, err := d.commands.Run(ctx, d.project, "gh", "pr", "update-branch", fmt.Sprintf("%d", prNumber), "--rebase")
	return err
}

func (d *Daemon) mergeQueuedPR(ctx context.Context, prNumber int) error {
	_, err := d.commands.Run(ctx, d.project, "gh", "pr", "merge", fmt.Sprintf("%d", prNumber), "--squash")
	return err
}

func (d *Daemon) lookupPRNumber(ctx context.Context, branch string) (int, error) {
	return d.github.lookupPRNumber(ctx, branch)
}

func (d *Daemon) lookupOpenPRNumber(ctx context.Context, branch string) (int, error) {
	return d.github.lookupOpenPRNumber(ctx, branch)
}

func (d *Daemon) isPRMerged(ctx context.Context, prNumber int) (bool, error) {
	return d.github.isPRMerged(ctx, prNumber)
}
