package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const mergeQueueChecksIntervalSecs = "10"
const conflictNudgePrompt = "PR has merge conflicts, rebase onto origin/main and push.\n"

func (d *Daemon) Enqueue(ctx context.Context, prNumber int) (MergeQueueActionResult, error) {
	if err := d.requireStarted(); err != nil {
		return MergeQueueActionResult{}, err
	}

	active, err := d.assignmentByPRNumber(prNumber)
	if err != nil {
		return MergeQueueActionResult{}, err
	}

	now := d.now()

	d.mu.Lock()
	if _, exists := d.mergeQueued[prNumber]; exists {
		d.mu.Unlock()
		return MergeQueueActionResult{}, fmt.Errorf("PR #%d is already queued for landing", prNumber)
	}
	d.mergeQueue = append(d.mergeQueue, prNumber)
	d.mergeQueued[prNumber] = struct{}{}
	position := len(d.mergeQueue)
	if d.activeMerge != 0 {
		position++
	}
	shouldStart := !d.mergeBusy
	if shouldStart {
		d.mergeBusy = true
	}
	d.mu.Unlock()

	d.emit(ctx, d.mergeQueueEvent(active, EventPREnqueued, prNumber, "pull request queued for landing", now))

	if shouldStart {
		d.wg.Add(1)
		go d.processMergeQueue()
	}

	return MergeQueueActionResult{
		Project:   d.project,
		PRNumber:  prNumber,
		Status:    "queued",
		Position:  position,
		UpdatedAt: now,
	}, nil
}

func (d *Daemon) processMergeQueue() {
	defer d.wg.Done()

	for {
		prNumber, active, ok := d.nextQueuedPR()
		if !ok {
			return
		}
		d.processQueuedPR(prNumber, active)
		d.completeQueuedPR(prNumber)
	}
}

func (d *Daemon) nextQueuedPR() (int, *assignment, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.mergeQueue) == 0 {
		d.mergeBusy = false
		return 0, nil, false
	}

	prNumber := d.mergeQueue[0]
	d.mergeQueue = d.mergeQueue[1:]
	d.activeMerge = prNumber
	return prNumber, d.assignmentByPRNumberLocked(prNumber), true
}

func (d *Daemon) completeQueuedPR(prNumber int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.mergeQueued, prNumber)
	if d.activeMerge == prNumber {
		d.activeMerge = 0
	}
}

func (d *Daemon) processQueuedPR(prNumber int, active *assignment) {
	ctx := d.mergeQueueContext()
	if active == nil {
		if ctx.Err() != nil {
			return
		}
		d.emit(ctx, d.mergeQueueEvent(nil, EventPRLandingFailed, prNumber, fmt.Sprintf("PR #%d is no longer tracked by an active assignment", prNumber), d.now()))
		return
	}

	d.emit(ctx, d.mergeQueueEvent(active, EventPRLandingStarted, prNumber, "processing queued PR landing", d.now()))

	if err := d.rebaseQueuedPR(ctx, prNumber); err != nil {
		d.handleQueuedPRFailure(ctx, active, prNumber, mergeQueueRebaseConflictPrompt(prNumber), err)
		return
	}
	if err := d.waitForQueuedPRChecks(ctx, prNumber); err != nil {
		d.handleQueuedPRFailure(ctx, active, prNumber, mergeQueueChecksFailedPrompt(prNumber), err)
		return
	}
	if err := d.mergeQueuedPR(ctx, prNumber); err != nil {
		d.handleQueuedPRFailure(ctx, active, prNumber, mergeQueueMergeFailedPrompt(prNumber), err)
	}
}

func (d *Daemon) mergeQueueContext() context.Context {
	if d.stopContext != nil {
		return d.stopContext
	}
	return context.Background()
}

func (d *Daemon) handlePRPoll(active *assignment) {
	if active.prNumber == 0 {
		prNumber, err := d.lookupPRNumber(active.ctx, active.task.Branch)
		if err != nil {
			return
		}
		if prNumber > 0 {
			if err := d.setPaneMetadata(active.ctx, active.pane.ID, assignmentMetadata(active.profile.Name, active.task.Branch, active.task.Issue, prNumber)); err != nil {
				return
			}
			active.prNumber = prNumber
			active.task.PRNumber = prNumber
			active.task.UpdatedAt = d.now()
			_ = d.state.PutTask(active.ctx, active.task)
			d.emit(active.ctx, Event{
				Time:         d.now(),
				Type:         EventPRDetected,
				Project:      d.project,
				Issue:        active.task.Issue,
				PaneID:       active.pane.ID,
				PaneName:     active.pane.Name,
				CloneName:    active.clone.Name,
				ClonePath:    active.clone.Path,
				Branch:       active.task.Branch,
				AgentProfile: active.profile.Name,
				PRNumber:     prNumber,
				Message:      "pull request detected",
			})
		}
	}

	if active.prNumber == 0 {
		return
	}

	d.handlePRChecksPoll(active)

	merged, err := d.isPRMerged(active.ctx, active.prNumber)
	if err != nil || !merged {
		d.handlePRMergeablePoll(active)
		d.handlePRReviewPoll(active)
		return
	}
	message := "pull request merged"
	if err := d.setIssueStatus(active.ctx, active.task.Issue, IssueStateDone); err != nil {
		message = fmt.Sprintf("pull request merged (failed to update Linear issue status: %v)", err)
	}

	d.emit(active.ctx, Event{
		Time:         d.now(),
		Type:         EventPRMerged,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		PRNumber:     active.prNumber,
		Message:      message,
	})
	if err := d.finishAssignment(active.ctx, active, TaskStatusDone, EventTaskCompleted, true); err != nil {
		d.emit(active.ctx, Event{
			Time:         d.now(),
			Type:         EventTaskCompletionFailed,
			Project:      d.project,
			Issue:        active.task.Issue,
			PaneID:       active.pane.ID,
			PaneName:     active.pane.Name,
			CloneName:    active.clone.Name,
			ClonePath:    active.clone.Path,
			Branch:       active.task.Branch,
			AgentProfile: active.profile.Name,
			PRNumber:     active.prNumber,
			Message:      err.Error(),
		})
	}
}

func (d *Daemon) rebaseQueuedPR(ctx context.Context, prNumber int) error {
	_, err := d.commands.Run(ctx, d.project, "gh", "pr", "update-branch", fmt.Sprintf("%d", prNumber), "--rebase")
	return err
}

func (d *Daemon) waitForQueuedPRChecks(ctx context.Context, prNumber int) error {
	_, err := d.commands.Run(ctx, d.project, "gh", "pr", "checks", fmt.Sprintf("%d", prNumber), "--required", "--watch", "--fail-fast", "--interval", mergeQueueChecksIntervalSecs)
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

func (d *Daemon) handlePRMergeablePoll(active *assignment) {
	state, ok, err := d.lookupPRMergeableState(active.ctx, active.prNumber)
	if err != nil || !ok {
		return
	}

	previousState := active.mergeableState()
	if previousState == "CONFLICTING" || state != "CONFLICTING" {
		active.setMergeableState(state)
		return
	}

	if err := d.amux.SendKeys(active.ctx, active.pane.ID, conflictNudgePrompt); err != nil {
		return
	}

	active.setMergeableState(state)

	d.emit(active.ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerNudgedConflict,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		PRNumber:     active.prNumber,
		Message:      strings.TrimSpace(conflictNudgePrompt),
	})
}

func (d *Daemon) lookupPRMergeableState(ctx context.Context, prNumber int) (string, bool, error) {
	output, err := d.commands.Run(ctx, d.project, "gh", "pr", "view", fmt.Sprintf("%d", prNumber), "--json", "mergeable")
	if err != nil {
		return "", false, err
	}
	if len(output) == 0 {
		return "", false, nil
	}

	var payload struct {
		Mergeable string `json:"mergeable"`
	}
	if err := json.Unmarshal(output, &payload); err != nil {
		return "", false, err
	}
	if payload.Mergeable == "" {
		return "", false, nil
	}
	return payload.Mergeable, true, nil
}
