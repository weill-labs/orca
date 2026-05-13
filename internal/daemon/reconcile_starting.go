package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const startingTaskDriftThreshold = 5 * time.Minute

func (d *Daemon) reconcileStartingTaskDrift(ctx context.Context, task Task) (ReconcileFinding, bool, error) {
	if !startingTaskPastDriftThreshold(task, d.now()) {
		return ReconcileFinding{}, false, nil
	}

	paneID := strings.TrimSpace(task.PaneID)
	if paneID != "" {
		exists, _, err := d.paneExists(ctx, paneID)
		if err != nil {
			return ReconcileFinding{}, false, fmt.Errorf("check pane for %s: %w", task.Issue, err)
		}
		if exists {
			return ReconcileFinding{}, false, nil
		}
	}

	finding := taskReconcileFinding(task, reconcilePRInfo{
		state:  reconcilePRStateNone,
		branch: strings.TrimSpace(task.Branch),
	})
	finding.Kind = ReconcileStartingZombie
	finding.Message = fmt.Sprintf("task has been starting for at least %s without a live worker pane; run `orca cancel %s` or `orca reconcile --fix` to cancel and clear it", startingTaskDriftThreshold, task.Issue)
	return finding, true, nil
}

func startingTaskPastDriftThreshold(task Task, now time.Time) bool {
	startedAt := task.UpdatedAt
	if startedAt.IsZero() {
		startedAt = task.CreatedAt
	}
	if startedAt.IsZero() {
		// No timestamp recorded: treat as immediately stale so the zombie can
		// be cleared, but only after the caller confirms there is no live pane.
		return true
	}
	return !now.Before(startedAt.Add(startingTaskDriftThreshold))
}
