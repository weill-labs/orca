package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	conflictNudgePrompt           = "PR has merge conflicts, rebase onto origin/main and push."
	relayMergeableInitialDelay    = 5 * time.Second
	prMergeableJSONFields         = "mergeable,mergeStateStatus"
	mergeableUnknownRetryDelay    = 5 * time.Second
	mergeableUnknownRetryMaxTries = 3
)

type prMergeabilityPayload struct {
	Mergeable        string `json:"mergeable"`
	MergeStateStatus string `json:"mergeStateStatus"`
}

func (d *Daemon) handleQueuedPRFailure(ctx context.Context, active ActiveAssignment, prNumber int, prompt string, err error) {
	if ctx.Err() != nil {
		return
	}

	_ = d.sendPromptAndEnter(ctx, active.Task.PaneID, prompt)
	d.emit(ctx, d.mergeQueueEvent(&active, EventPRLandingFailed, prNumber, err.Error(), d.now()))
}

func (d *Daemon) handlePRMergeablePoll(ctx context.Context, update *TaskStateUpdate, profile AgentProfile) {
	state, ok, err := d.lookupPRMergeableState(ctx, prProjectForTask(update.Active.Task), update.Active.Task.PRNumber)
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

	state, ok, err := d.lookupPRMergeableStateAfterMerge(ctx, prProjectForTask(active.Task), active.Task.PRNumber)
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
	payload, ok, err := d.lookupPRMergeability(ctx, projectPath, prNumber)
	if err != nil || !ok {
		return "", ok, err
	}

	state, ok, retry := resolvePRMergeableState(payload)
	if ok || !retry {
		return state, ok, nil
	}

	for attempt := 0; attempt < mergeableUnknownRetryMaxTries; attempt++ {
		if err := d.sleep(ctx, mergeableUnknownRetryDelay); err != nil {
			return "", false, err
		}

		payload, ok, err = d.lookupPRMergeability(ctx, projectPath, prNumber)
		if err != nil || !ok {
			return "", ok, err
		}

		state, ok, retry = resolvePRMergeableState(payload)
		if ok || !retry {
			return state, ok, nil
		}
	}

	return fallbackPRMergeableState(payload)
}

func (d *Daemon) lookupPRMergeableStateAfterMerge(ctx context.Context, projectPath string, prNumber int) (string, bool, error) {
	if err := d.sleep(ctx, relayMergeableInitialDelay); err != nil {
		return "", false, err
	}

	return d.lookupPRMergeableState(ctx, projectPath, prNumber)
}

func (d *Daemon) lookupPRMergeability(ctx context.Context, projectPath string, prNumber int) (prMergeabilityPayload, bool, error) {
	payload, ok, err := d.gitHubClientForContext(ctx, projectPath).lookupPRMergeability(ctx, prNumber)
	if err != nil {
		return prMergeabilityPayload{}, false, err
	}
	if !ok {
		return prMergeabilityPayload{}, false, nil
	}
	d.logPRMergeability(prNumber, payload)

	if payload.Mergeable == "" && payload.MergeStateStatus == "" {
		return prMergeabilityPayload{}, false, nil
	}

	return payload, true, nil
}

func (d *Daemon) logPRMergeability(prNumber int, payload prMergeabilityPayload) {
	if d.logf == nil {
		return
	}

	d.logf(
		"pr mergeability: pr=%d mergeable=%s mergeStateStatus=%s",
		prNumber,
		formatPRMergeabilityField(payload.Mergeable),
		formatPRMergeabilityField(payload.MergeStateStatus),
	)
}

func resolvePRMergeableState(payload prMergeabilityPayload) (string, bool, bool) {
	if payload.Mergeable == "UNKNOWN" {
		return "", false, true
	}
	if payload.MergeStateStatus == "DIRTY" && payload.Mergeable != "CONFLICTING" {
		return "CONFLICTING", true, false
	}
	if payload.Mergeable == "" {
		return "", false, false
	}
	return payload.Mergeable, true, false
}

func fallbackPRMergeableState(payload prMergeabilityPayload) (string, bool, error) {
	if payload.MergeStateStatus == "DIRTY" && payload.Mergeable != "CONFLICTING" {
		return "CONFLICTING", true, nil
	}
	return "", false, nil
}

func formatPRMergeabilityField(value string) string {
	if strings.TrimSpace(value) == "" {
		return "<empty>"
	}
	return value
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
