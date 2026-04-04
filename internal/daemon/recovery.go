package daemon

import (
	"context"
	"errors"
	"path/filepath"
)

func (d *Daemon) reconcileActiveAssignments(ctx context.Context) {
	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err != nil {
		return
	}

	panes, err := d.amux.ListPanes(ctx)
	if err != nil {
		return
	}

	livePaneIDs := make(map[string]struct{}, len(panes))
	for _, pane := range panes {
		if pane.ID == "" {
			continue
		}
		livePaneIDs[pane.ID] = struct{}{}
	}

	for _, active := range assignments {
		if ctx.Err() != nil {
			return
		}
		if _, ok := livePaneIDs[active.Task.PaneID]; ok {
			continue
		}
		_ = d.failAssignment(ctx, active, EventTaskFailed, "worker pane missing on daemon startup")
	}
}

func (d *Daemon) failAssignment(ctx context.Context, active ActiveAssignment, eventType, message string) error {
	cleanupCtx := context.WithoutCancel(ctx)
	now := d.now()

	task := active.Task
	task.Status = TaskStatusFailed
	task.UpdatedAt = now

	clone := Clone{
		Name: active.Task.CloneName,
		Path: active.Task.ClonePath,
	}
	if clone.Name == "" && clone.Path != "" {
		clone.Name = filepath.Base(clone.Path)
	}

	var result error
	result = errors.Join(result, d.cleanupCloneAndRelease(cleanupCtx, clone, active.Task.Branch))
	result = errors.Join(result, d.state.PutTask(cleanupCtx, task))
	result = errors.Join(result, d.state.DeleteWorker(cleanupCtx, d.project, active.Task.PaneID))
	if active.Task.PRNumber > 0 {
		if err := d.state.DeleteMergeEntry(cleanupCtx, d.project, active.Task.PRNumber); err != nil && !errors.Is(err, ErrTaskNotFound) {
			result = errors.Join(result, err)
		}
	}

	profile, err := d.profileForTask(cleanupCtx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}

	paneName := active.Task.PaneName
	if paneName == "" {
		paneName = active.Worker.PaneName
	}

	d.emit(cleanupCtx, Event{
		Time:         now,
		Type:         eventType,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     paneName,
		CloneName:    clone.Name,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      message,
	})

	return result
}
