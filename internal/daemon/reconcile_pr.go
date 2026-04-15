package daemon

import (
	"context"
	"strings"
	"time"
)

const missingPRNumberReconcileInterval = 5 * time.Minute

func (d *Daemon) reconcileMissingPRNumbers(ctx context.Context) {
	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err != nil {
		return
	}

	for _, active := range assignments {
		update := d.reconcileMissingPRNumber(ctx, active)
		if !update.TaskChanged && !update.WorkerChanged && len(update.Events) == 0 && !update.PRMerged {
			continue
		}
		d.applyTaskStateUpdate(ctx, update)
	}
}

func (d *Daemon) reconcileMissingPRNumber(ctx context.Context, active ActiveAssignment) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}
	if active.Task.PRNumber != 0 || strings.TrimSpace(active.Task.Branch) == "" {
		return update
	}

	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return update
	}

	prNumber, merged, err := d.lookupOpenOrMergedPRNumber(ctx, active.Task.Project, active.Task.Branch)
	if err != nil || prNumber == 0 {
		return update
	}

	now := d.now()
	update.Active.Task.PRNumber = prNumber
	update.Active.Task.UpdatedAt = now
	update.TaskChanged = true
	update.Active.Worker.LastPRNumber = prNumber
	update.Active.Worker.LastPushAt = now
	update.WorkerChanged = true
	update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventPRDetected, "pull request detected"))
	update.PRMerged = merged
	return update
}

func shouldRunMissingPRNumberReconciliation(lastRun, now time.Time) bool {
	if now.IsZero() {
		return false
	}
	if lastRun.IsZero() {
		return true
	}
	return !now.Before(lastRun.Add(missingPRNumberReconcileInterval))
}
