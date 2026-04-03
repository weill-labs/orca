package daemon

import (
	"context"
	"fmt"
)

func (d *Daemon) handleQueuedPRFailure(ctx context.Context, active *assignment, prNumber int, prompt string, err error) {
	if ctx.Err() != nil {
		return
	}

	_ = d.amux.SendKeys(ctx, active.pane.ID, ensureTrailingNewline(prompt))
	d.emit(ctx, d.mergeQueueEvent(active, EventPRLandingFailed, prNumber, err.Error(), d.now()))
}

func mergeQueueRebaseConflictPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not rebase PR #%d onto main. Resolve the conflicts, push an update, and re-run `orca enqueue %d` when ready.\n", prNumber, prNumber)
}

func mergeQueueChecksFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue rebased PR #%d onto main, but required checks did not pass. Fix the branch, push an update, and re-run `orca enqueue %d` when ready.\n", prNumber, prNumber)
}

func mergeQueueMergeFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not land PR #%d after verification. Check the PR state, push an update if needed, and re-run `orca enqueue %d` when ready.\n", prNumber, prNumber)
}
