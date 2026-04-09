package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const conflictNudgePrompt = "PR has merge conflicts, rebase onto origin/main and push."

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
			return
		}

		update.Active.Worker.LastMergeableState = state
		update.Active.Worker.LastSeenAt = now
		update.WorkerChanged = true
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerNudgedConflict, strings.TrimSpace(conflictNudgePrompt)))
	})
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

func mergeQueueRebaseConflictPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not rebase PR #%d onto main. Resolve the conflicts, push an update, and re-run `orca enqueue %d` when ready.", prNumber, prNumber)
}

func mergeQueueChecksFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue rebased PR #%d onto main, but required checks did not pass. Fix the branch, push an update, and re-run `orca enqueue %d` when ready.", prNumber, prNumber)
}

func mergeQueueMergeFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not land PR #%d after verification. Check the PR state, push an update if needed, and re-run `orca enqueue %d` when ready.", prNumber, prNumber)
}
