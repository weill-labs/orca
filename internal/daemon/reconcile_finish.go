package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

func fixedReconcileFindingMessage(finding ReconcileFinding) string {
	switch finding.Kind {
	case ReconcileRecoverableGhost:
		return "task marked done and clone released; worker pane was already missing"
	case ReconcileStuckCleanup:
		pane := reconcileFindingPaneRef(finding)
		return fmt.Sprintf("task marked done and clone released; live pane %s left running for human inspection and manual cleanup", pane)
	default:
		return finding.Message
	}
}

func reconcileFindingPaneRef(finding ReconcileFinding) string {
	for _, value := range []string{finding.PaneName, finding.PaneID} {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return "unknown"
}

func (d *Daemon) finishMergedAssignmentWithoutPane(ctx context.Context, active ActiveAssignment) error {
	cleanupCtx := d.cleanupContext(ctx)
	d.stopTaskMonitorForProject(active.Task.Project, active.Task.Issue)

	profile := d.reconcileAssignmentProfile(cleanupCtx, active.Task)
	d.emit(cleanupCtx, Event{
		Time:         d.now(),
		Type:         EventWorkerPostmortem,
		Project:      active.Task.Project,
		Issue:        active.Task.Issue,
		WorkerID:     active.Worker.WorkerID,
		PaneID:       active.Task.PaneID,
		PaneName:     assignmentPaneName(active.Task, active.Worker),
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      "postmortem skipped: worker pane missing",
	})

	return d.completeMergedAssignmentForReconcile(cleanupCtx, active, profile, "task finished")
}

func (d *Daemon) finishMergedAssignmentWithLivePaneFromReconcile(ctx context.Context, active ActiveAssignment) error {
	var result error
	cleanupCtx := d.cleanupContext(ctx)
	d.stopTaskMonitorForProject(active.Task.Project, active.Task.Issue)

	profile := d.reconcileAssignmentProfile(cleanupCtx, active.Task)
	paneMessage := "postmortem skipped: reconcile left live pane running for human inspection"
	if paneID := strings.TrimSpace(active.Task.PaneID); paneID != "" {
		if err := d.markLivePaneCompletedForReconcile(cleanupCtx, active, paneID); err != nil {
			result = errors.Join(result, err)
			paneMessage = fmt.Sprintf("%s; pane metadata update failed: %v", paneMessage, err)
		}
	}

	d.emit(cleanupCtx, Event{
		Time:         d.now(),
		Type:         EventWorkerPostmortem,
		Project:      active.Task.Project,
		Issue:        active.Task.Issue,
		WorkerID:     active.Worker.WorkerID,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      paneMessage,
	})

	result = errors.Join(result, d.completeMergedAssignmentForReconcile(cleanupCtx, active, profile, "task finished"))
	return result
}

func (d *Daemon) markLivePaneCompletedForReconcile(ctx context.Context, active ActiveAssignment, paneID string) error {
	metadata, err := d.completionPaneMetadata(ctx, active, true)
	if err != nil {
		return fmt.Errorf("mark live pane complete: %w", err)
	}
	if err := d.setPaneMetadata(ctx, paneID, metadata); err != nil {
		return fmt.Errorf("mark live pane complete: %w", err)
	}
	return nil
}

func (d *Daemon) reconcileAssignmentProfile(ctx context.Context, task Task) AgentProfile {
	profile, err := d.profileForTask(ctx, task)
	if err != nil {
		return AgentProfile{Name: task.AgentProfile}
	}
	return profile
}

func (d *Daemon) completeMergedAssignmentForReconcile(ctx context.Context, active ActiveAssignment, profile AgentProfile, message string) error {
	var result error
	clone := Clone{
		Name: active.Task.CloneName,
		Path: active.Task.ClonePath,
	}
	if clone.Name == "" && clone.Path != "" {
		clone.Name = filepath.Base(clone.Path)
	}
	result = errors.Join(result, d.cleanupCloneAndReleaseForProject(ctx, active.Task.Project, clone, active.Task.Branch))

	active.Task.Status = TaskStatusDone
	active.Task.State = TaskStateDone
	active.Task.UpdatedAt = d.now()
	result = errors.Join(result, d.state.PutTask(ctx, active.Task))
	result = errors.Join(result, d.releaseWorkerClaim(ctx, active.Worker))
	if active.Task.PRNumber > 0 {
		if err := d.state.DeleteMergeEntry(ctx, active.Task.Project, active.Task.PRNumber); err != nil && !errors.Is(err, ErrTaskNotFound) {
			result = errors.Join(result, err)
		}
	}
	d.requestRelayReconnect()
	d.completeWorkSource(ctx, active, TaskStatusDone)

	if strings.TrimSpace(message) == "" {
		message = "task finished"
	}
	d.emit(ctx, d.assignmentEvent(active, profile, EventTaskCompleted, message))
	return result
}
