package daemon

import (
	"context"
	"strings"
)

func (d *Daemon) cancellationPaneMissing(ctx context.Context, active ActiveAssignment) (bool, error) {
	paneID := strings.TrimSpace(active.Task.PaneID)
	if paneID == "" {
		return true, nil
	}
	exists, _, err := d.paneExists(ctx, paneID)
	if err != nil {
		return false, err
	}
	return !exists, nil
}

func (d *Daemon) emitSkippedPostmortemForMissingPane(ctx context.Context, active ActiveAssignment) {
	profile := d.reconcileAssignmentProfile(ctx, active.Task)
	d.emit(ctx, Event{
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
}
