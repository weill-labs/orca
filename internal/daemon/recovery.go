package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

func (d *Daemon) reconcileNonTerminalAssignments(ctx context.Context) {
	tasks, err := d.state.NonTerminalTasks(ctx, d.project)
	if err != nil {
		return
	}

	for _, task := range tasks {
		if ctx.Err() != nil {
			return
		}
		d.reconcileTaskOnStartup(ctx, task)
	}
}

func (d *Daemon) reconcileTaskOnStartup(ctx context.Context, task Task) {
	paneID := strings.TrimSpace(task.PaneID)
	if paneID == "" {
		_ = d.failTaskWithoutWorker(ctx, task, "worker pane missing on daemon startup")
		return
	}

	worker, err := d.state.WorkerByPane(ctx, d.project, paneID)
	if err != nil {
		_ = d.failTaskWithoutWorker(ctx, task, "persisted worker missing on daemon startup")
		return
	}

	active := ActiveAssignment{
		Task:   task,
		Worker: worker,
	}

	exists, err := d.amux.PaneExists(ctx, active.Task.PaneID)
	if err != nil {
		d.handleTaskStartupPaneError(ctx, active, "worker pane liveness check failed on daemon startup", err)
		return
	}
	if !exists {
		_ = d.failAssignment(ctx, active, EventTaskFailed, "worker pane missing on daemon startup")
		return
	}

	d.reconcileWorkerHealthFromPaneMetadata(ctx, &active)

	snapshot, err := d.amux.CapturePane(ctx, active.Task.PaneID)
	if err != nil {
		d.handleTaskStartupPaneError(ctx, active, "worker pane capture failed on daemon startup", err)
		return
	}
	if snapshot.Exited {
		if active.Task.Status == TaskStatusStarting {
			_ = d.failAssignment(ctx, active, EventTaskFailed, "worker pane exited on daemon startup")
			return
		}

		profile, err := d.profileForTask(ctx, active.Task)
		if err != nil {
			profile = AgentProfile{Name: active.Task.AgentProfile}
		}
		if d.shouldEscalateExitedPane(snapshot, d.now()) {
			d.handleExitedPaneCapture(ctx, active, profile, snapshot, d.now())
		}
		return
	}

	if active.Task.Status == TaskStatusStarting {
		active.Task.Status = TaskStatusActive
		active.Task.UpdatedAt = d.now()
		_ = d.state.PutTask(ctx, active.Task)
	}
}

func (d *Daemon) reconcileWorkerHealthFromPaneMetadata(ctx context.Context, active *ActiveAssignment) {
	metadata, err := d.amux.Metadata(ctx, active.Task.PaneID)
	if err != nil {
		return
	}
	if !strings.EqualFold(strings.TrimSpace(metadata["status"]), WorkerHealthEscalated) {
		return
	}
	if active.Worker.Health == WorkerHealthEscalated {
		return
	}

	active.Worker.Health = WorkerHealthEscalated
	active.Worker.UpdatedAt = d.now()
	_ = d.state.PutWorker(ctx, active.Worker)
}

func (d *Daemon) handleTaskStartupPaneError(ctx context.Context, active ActiveAssignment, message string, err error) {
	detail := fmt.Sprintf("%s: %v", message, err)
	if active.Task.Status == TaskStatusStarting {
		_ = d.failAssignment(ctx, active, EventTaskFailed, detail)
		return
	}

	d.escalateAssignmentOnStartupError(ctx, active, detail)
}

func (d *Daemon) escalateAssignmentOnStartupError(ctx context.Context, active ActiveAssignment, message string) {
	if active.Worker.Health == WorkerHealthEscalated {
		return
	}

	now := d.now()
	active.Worker.Health = WorkerHealthEscalated
	active.Worker.UpdatedAt = now
	active.Task.UpdatedAt = now

	_ = d.state.PutWorker(ctx, active.Worker)
	_ = d.state.PutTask(ctx, active.Task)

	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}

	d.emit(ctx, Event{
		Time:         now,
		Type:         EventWorkerEscalated,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		Retry:        active.Worker.NudgeCount,
		Message:      message,
	})
}

func (d *Daemon) failTaskWithoutWorker(ctx context.Context, task Task, message string) error {
	active := ActiveAssignment{
		Task: task,
		Worker: Worker{
			Project:      d.project,
			PaneID:       task.PaneID,
			PaneName:     task.PaneName,
			Issue:        task.Issue,
			ClonePath:    task.ClonePath,
			AgentProfile: task.AgentProfile,
		},
	}
	return d.failAssignment(ctx, active, EventTaskFailed, message)
}

func (d *Daemon) failAssignment(ctx context.Context, active ActiveAssignment, eventType, message string) error {
	cleanupCtx := context.WithoutCancel(ctx)
	now := d.now()
	d.stopTaskMonitor(active.Task.Issue)

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
	if clone.Path != "" {
		result = errors.Join(result, d.cleanupCloneAndRelease(cleanupCtx, clone, active.Task.Branch))
	}
	result = errors.Join(result, d.state.PutTask(cleanupCtx, task))
	if active.Task.PaneID != "" {
		if err := d.state.DeleteWorker(cleanupCtx, d.project, active.Task.PaneID); err != nil && !errors.Is(err, ErrWorkerNotFound) {
			result = errors.Join(result, err)
		}
	}
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
