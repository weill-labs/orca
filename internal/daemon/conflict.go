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

func (d *Daemon) handlePRMergeablePoll(ctx context.Context, active ActiveAssignment, profile AgentProfile) {
	state, ok, err := d.lookupPRMergeableState(ctx, active.Task.PRNumber)
	if err != nil || !ok {
		return
	}

	previousState := active.Worker.LastMergeableState
	if previousState == "CONFLICTING" || state != "CONFLICTING" {
		if previousState != state {
			active.Worker.LastMergeableState = state
			active.Worker.UpdatedAt = d.now()
			_ = d.state.PutWorker(ctx, active.Worker)
		}
		return
	}

	if err := d.amux.SendKeys(ctx, active.Task.PaneID, conflictNudgePrompt); err != nil {
		return
	}
	if err := d.amux.WaitIdle(ctx, active.Task.PaneID, defaultAgentHandshakeTimeout); err != nil {
		return
	}
	if err := d.amux.SendKeys(ctx, active.Task.PaneID, "Enter"); err != nil {
		return
	}

	active.Worker.LastMergeableState = state
	active.Worker.UpdatedAt = d.now()
	_ = d.state.PutWorker(ctx, active.Worker)

	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerNudgedConflict,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
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

func mergeQueueRebaseConflictPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not rebase PR #%d onto main. Resolve the conflicts, push an update, and re-run `orca enqueue %d` when ready.", prNumber, prNumber)
}

func mergeQueueChecksFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue rebased PR #%d onto main, but required checks did not pass. Fix the branch, push an update, and re-run `orca enqueue %d` when ready.", prNumber, prNumber)
}

func mergeQueueMergeFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not land PR #%d after verification. Check the PR state, push an update if needed, and re-run `orca enqueue %d` when ready.", prNumber, prNumber)
}
