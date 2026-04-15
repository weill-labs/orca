package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	conflictNudgePrompt            = "PR has merge conflicts, rebase onto origin/main and push."
	relayMergeableInitialDelay     = 5 * time.Second
	relayMergeableRetryInterval    = 10 * time.Second
	relayMergeableRetryMaxAttempts = 3
)

func (d *Daemon) handleQueuedPRFailure(ctx context.Context, active ActiveAssignment, prNumber int, prompt string, err error) {
	if ctx.Err() != nil {
		return
	}

	_ = d.sendPromptAndEnter(ctx, active.Task.PaneID, prompt)
	d.emit(ctx, d.mergeQueueEvent(&active, EventPRLandingFailed, prNumber, err.Error(), d.now()))
}

func (d *Daemon) handlePRMergeablePoll(ctx context.Context, update *TaskStateUpdate, profile AgentProfile) {
	state, ok, err := d.lookupPRMergeableState(ctx, update.Active.Task.Project, update.Active.Task.PRNumber)
	if err != nil || !ok {
		return
	}
	d.applyPRMergeableState(update, profile, state)
}

func (d *Daemon) applyPRMergeableState(update *TaskStateUpdate, profile AgentProfile, state string) {
	now := d.now()

	previousState := update.Active.Worker.LastMergeableState
	if previousState == "CONFLICTING" || state != "CONFLICTING" {
		if previousState != state {
			update.Active.Worker.LastMergeableState = state
			update.Active.Worker.LastSeenAt = now
			update.WorkerChanged = true
		}
		return
	}

	update.queueNudge(func(ctx context.Context, d *Daemon, update *TaskStateUpdate) {
		if err := d.sendPromptAndEnter(ctx, update.Active.Task.PaneID, conflictNudgePrompt); err != nil {
			if isPaneGoneError(err) {
				d.escalateTaskState(update, profile, "worker pane missing during conflict nudge", now)
			}
			return
		}

		update.Active.Worker.LastMergeableState = state
		update.Active.Worker.LastSeenAt = now
		update.WorkerChanged = true
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerNudgedConflict, strings.TrimSpace(conflictNudgePrompt)))
	})
}

func (d *Daemon) checkTaskImmediateMergeConflictPoll(ctx context.Context, active ActiveAssignment) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}
	if active.Task.PRNumber == 0 {
		return update
	}

	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return update
	}

	state, ok, err := d.lookupPRMergeableStateAfterMerge(ctx, active.Task.Project, active.Task.PRNumber)
	if err != nil {
		d.appendGitHubRateLimitEvent(&update, profile, err)
		return update
	}
	if !ok {
		return update
	}

	d.applyPRMergeableState(&update, profile, state)
	return update
}

func (d *Daemon) lookupPRMergeableState(ctx context.Context, projectPath string, prNumber int) (string, bool, error) {
	output, err := d.commandRunner(ctx).Run(ctx, projectPath, "gh", "pr", "view", fmt.Sprintf("%d", prNumber), "--json", "mergeable")
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

func (d *Daemon) lookupPRMergeableStateAfterMerge(ctx context.Context, projectPath string, prNumber int) (string, bool, error) {
	if err := d.sleep(ctx, relayMergeableInitialDelay); err != nil {
		return "", false, err
	}

	state, ok, err := d.lookupPRMergeableState(ctx, projectPath, prNumber)
	if err != nil || !ok || state != "UNKNOWN" {
		return state, ok, err
	}

	for attempt := 0; attempt < relayMergeableRetryMaxAttempts; attempt++ {
		if err := d.sleep(ctx, relayMergeableRetryInterval); err != nil {
			return "", false, err
		}

		state, ok, err = d.lookupPRMergeableState(ctx, projectPath, prNumber)
		if err != nil || !ok || state != "UNKNOWN" {
			return state, ok, err
		}
	}

	return state, ok, err
}

func mergeQueueRebaseConflictPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not rebase PR #%d onto main. Resolve the conflicts, push an update, and re-run `orca enqueue %d` when ready.", prNumber, prNumber)
}

func mergeQueueChecksFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue rebased PR #%d onto main, but required checks did not pass. Fix the branch, push an update, and re-run `orca enqueue %d` when ready.", prNumber, prNumber)
}

func mergeQueueMergeFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not land PR #%d after verification. Check the PR state, push an update if needed, and re-run `orca enqueue %d` when ready.", prNumber, prNumber)
}
