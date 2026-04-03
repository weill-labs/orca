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

func (d *Daemon) handlePRPoll(ctx context.Context, active ActiveAssignment) {
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return
	}

	if active.Task.PRNumber == 0 {
		prNumber, err := d.lookupPRNumber(ctx, active.Task.Branch)
		if err != nil {
			return
		}
		if prNumber > 0 {
			if err := d.setPaneMetadata(ctx, active.Task.PaneID, assignmentMetadata(profile.Name, active.Task.Branch, active.Task.Issue, prNumber)); err != nil {
				return
			}
			active.Task.PRNumber = prNumber
			active.Task.UpdatedAt = d.now()
			_ = d.state.PutTask(ctx, active.Task)
			d.emit(ctx, Event{
				Time:         d.now(),
				Type:         EventPRDetected,
				Project:      d.project,
				Issue:        active.Task.Issue,
				PaneID:       active.Task.PaneID,
				PaneName:     active.Task.PaneName,
				CloneName:    active.Task.CloneName,
				ClonePath:    active.Task.ClonePath,
				Branch:       active.Task.Branch,
				AgentProfile: profile.Name,
				PRNumber:     prNumber,
				Message:      "pull request detected",
			})
		}
	}

	if active.Task.PRNumber == 0 {
		return
	}
	if entry, err := d.state.MergeEntry(ctx, d.project, active.Task.PRNumber); err == nil && entry != nil {
		return
	}

	d.handlePRChecksPoll(ctx, active, profile)

	merged, err := d.isPRMerged(ctx, active.Task.PRNumber)
	if err != nil || !merged {
		d.handlePRMergeablePoll(ctx, active, profile)
		d.handlePRReviewPoll(ctx, active, profile)
		return
	}

	message := "pull request merged"
	if err := d.setIssueStatus(ctx, active.Task.Issue, IssueStateDone); err != nil {
		message = fmt.Sprintf("pull request merged (failed to update Linear issue status: %v)", err)
	}

	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventPRMerged,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      message,
	})
	if err := d.finishAssignment(ctx, active, TaskStatusDone, EventTaskCompleted, true); err != nil {
		d.emit(ctx, Event{
			Time:         d.now(),
			Type:         EventTaskCompletionFailed,
			Project:      d.project,
			Issue:        active.Task.Issue,
			PaneID:       active.Task.PaneID,
			PaneName:     active.Task.PaneName,
			CloneName:    active.Task.CloneName,
			ClonePath:    active.Task.ClonePath,
			Branch:       active.Task.Branch,
			AgentProfile: profile.Name,
			PRNumber:     active.Task.PRNumber,
			Message:      err.Error(),
		})
	}
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
