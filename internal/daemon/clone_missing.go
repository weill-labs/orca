package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	ReconcileCloneMissing   = "clone_missing"
	ReconcileClonePathError = "clone_path_error"
)

func taskManagedClonePathUnavailable(task Task) (string, bool, error) {
	seen := make(map[string]bool)
	for _, candidate := range []string{task.ClonePath, task.PRRepo} {
		clonePath := strings.TrimSpace(candidate)
		if clonePath == "" || seen[clonePath] {
			continue
		}
		seen[clonePath] = true
		if !isOrcaPoolClonePath(clonePath) {
			continue
		}
		unavailable, err := managedClonePathUnavailable(clonePath)
		if err != nil || unavailable {
			return clonePath, unavailable, err
		}
	}
	return "", false, nil
}

func managedClonePathUnavailable(clonePath string) (bool, error) {
	info, err := os.Stat(clonePath)
	if err == nil {
		return !info.IsDir(), nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	return false, err
}

func isOrcaPoolClonePath(clonePath string) bool {
	clonePath = filepath.ToSlash(filepath.Clean(strings.TrimSpace(clonePath)))
	return strings.Contains(clonePath, "/"+filepath.ToSlash(orcaPoolSubdir)+"/")
}

func cloneMissingRecoveryMessage(issue, clonePath string) string {
	return fmt.Sprintf("clone path %q is missing; restoring the directory alone does not resume polling; run `orca cancel %s` before reassigning", clonePath, issue)
}

func clonePathInspectionErrorMessage(issue, clonePath string, err error) string {
	return fmt.Sprintf("clone path %q could not be inspected: %v; fix filesystem access or run `orca cancel %s` before reassigning", clonePath, err, issue)
}

func (d *Daemon) markClonePathMissing(update *TaskStateUpdate, profile AgentProfile, clonePath string, now time.Time) {
	if update == nil {
		return
	}

	firstDetected := update.Active.Task.State != TaskStateCloneMissing
	message := cloneMissingRecoveryMessage(update.Active.Task.Issue, clonePath)

	if activeAssignmentHasWorkerIdentity(update.Active) && update.Active.Worker.Health != WorkerHealthEscalated {
		update.Active.Worker.Health = WorkerHealthEscalated
		update.Active.Worker.LastSeenAt = now
		update.WorkerChanged = true

		event := d.assignmentEvent(update.Active, profile, EventWorkerEscalated, message)
		event.Retry = update.Active.Worker.NudgeCount
		update.Events = append(update.Events, event)
	}
	if setTaskState(&update.Active.Task, TaskStateCloneMissing, now) {
		update.TaskChanged = true
	}
	if firstDetected {
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventPRPollCloneMissing, message))
	}
}

func activeAssignmentHasWorkerIdentity(active ActiveAssignment) bool {
	return strings.TrimSpace(active.Worker.WorkerID) != "" ||
		strings.TrimSpace(active.Worker.PaneID) != "" ||
		strings.TrimSpace(active.Worker.PaneName) != ""
}

func (d *Daemon) markClonePathMissingNow(ctx context.Context, active ActiveAssignment, messageClonePath string) {
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}

	update := TaskStateUpdate{Active: active}
	d.markClonePathMissing(&update, profile, messageClonePath, d.now())
	d.applyTaskStateUpdate(ctx, update)
}

func (d *Daemon) reconcileMissingClonePathDrift(task Task) (ReconcileFinding, bool, error) {
	clonePath, unavailable, err := taskManagedClonePathUnavailable(task)
	if err != nil {
		finding := taskReconcileFinding(task, reconcilePRInfo{
			state:  reconcilePRStateNone,
			branch: strings.TrimSpace(task.Branch),
		})
		finding.Kind = ReconcileClonePathError
		finding.Message = clonePathInspectionErrorMessage(task.Issue, clonePath, err)
		return finding, true, nil
	}
	if !unavailable {
		return ReconcileFinding{}, false, nil
	}

	finding := taskReconcileFinding(task, reconcilePRInfo{
		state:  reconcilePRStateNone,
		branch: strings.TrimSpace(task.Branch),
	})
	finding.Kind = ReconcileCloneMissing
	finding.Message = cloneMissingRecoveryMessage(task.Issue, clonePath)
	return finding, true, nil
}

func (d *Daemon) reconcileMissingClonePathOnStartup(ctx context.Context, task Task) bool {
	clonePath, unavailable, err := taskManagedClonePathUnavailable(task)
	if err != nil {
		if d.logf != nil {
			d.logf("missing clone path reconciliation failed for %s: %v", task.Issue, err)
		}
		return false
	}
	if !unavailable {
		return false
	}

	d.markClonePathMissingNow(ctx, d.reconcileActiveAssignment(ctx, task), clonePath)
	return true
}
