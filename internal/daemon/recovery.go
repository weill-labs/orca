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

func (d *Daemon) reconcileStrandedMergedTasks(ctx context.Context) {
	tasks, err := d.state.NonTerminalTasks(ctx, d.project)
	if err != nil {
		if d.logf != nil {
			d.logf("stranded merged task reconciliation failed: %v", err)
		}
		return
	}

	for _, task := range tasks {
		if ctx.Err() != nil {
			return
		}
		if !isStrandedMergedTask(task) {
			continue
		}

		active := d.reconcileActiveAssignment(ctx, task)
		if err := d.finishAssignmentWithMessage(ctx, active, TaskStatusDone, EventTaskCompleted, true, "task finished"); err != nil && d.logf != nil {
			d.logf("stranded merged task cleanup failed for %s: %v", task.Issue, err)
		}
	}
}

func isStrandedMergedTask(task Task) bool {
	if !taskBlocksAssignment(task.Status) {
		return false
	}

	switch strings.TrimSpace(task.State) {
	case TaskStateMerged, TaskStateDone:
		return true
	default:
		return false
	}
}

func (d *Daemon) releaseStalePoolClones(ctx context.Context) {
	occupancies, err := d.state.StaleCloneOccupancies(ctx, d.project)
	if err != nil {
		if d.logf != nil {
			d.logf("stale clone occupancy query failed: %v", err)
		}
		return
	}

	for _, occupancy := range occupancies {
		if ctx.Err() != nil {
			return
		}

		clone := Clone{
			Path:          occupancy.Path,
			CurrentBranch: occupancy.CurrentBranch,
			AssignedTask:  occupancy.AssignedTask,
		}
		projectPath := firstNonEmpty(occupancy.Project, d.project)
		branch := firstNonEmpty(occupancy.CurrentBranch, occupancy.AssignedTask)
		if err := d.cleanupCloneAndReleaseForProject(ctx, projectPath, clone, branch); err != nil && d.logf != nil {
			d.logf("release stale pool clone %q for %q: %v", occupancy.Path, projectPath, err)
		}
	}
}

func (d *Daemon) reconcileTaskOnStartup(ctx context.Context, task Task) {
	paneID := strings.TrimSpace(task.PaneID)
	if paneID == "" {
		_ = d.failTaskWithoutWorker(ctx, task, "worker pane missing on daemon startup")
		return
	}

	projectPath := d.projectPathForTask(task)
	workerID := stableWorkerRef(task, Worker{})

	var worker Worker
	var err error
	if workerID != "" {
		worker, err = d.state.WorkerByID(ctx, projectPath, workerID)
	}
	if workerID == "" || errors.Is(err, ErrWorkerNotFound) {
		worker, err = d.state.WorkerByPane(ctx, projectPath, paneID)
	}
	if err != nil {
		_ = d.failTaskWithoutWorker(ctx, task, "persisted worker missing on daemon startup")
		return
	}
	if err := d.normalizeStoredPaneRef(ctx, &task, &worker); err != nil {
		d.escalateTaskStartupPaneError(ctx, ActiveAssignment{Task: task, Worker: worker}, "worker pane normalization failed on daemon startup", err)
		return
	}

	active := ActiveAssignment{
		Task:   task,
		Worker: worker,
	}

	exists, _, err := d.paneExists(ctx, active.Task.PaneID)
	if err != nil {
		d.escalateTaskStartupPaneError(ctx, active, "worker pane liveness check failed on daemon startup", err)
		return
	}
	if !exists {
		d.escalateAssignmentError(ctx, active, "worker pane missing on daemon startup")
		return
	}

	d.reconcileWorkerHealthFromPaneMetadata(ctx, &active)

	snapshot, err := d.amux.CapturePane(ctx, active.Task.PaneID)
	if err != nil {
		d.escalateTaskStartupPaneError(ctx, active, "worker pane capture failed on daemon startup", err)
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
		d.handleExitedPaneCapture(ctx, active, profile, snapshot, d.now())
		return
	}

	if active.Task.Status == TaskStatusStarting {
		active.Task.Status = TaskStatusActive
		active.Task.UpdatedAt = d.now()
		_ = d.state.PutTask(ctx, active.Task)
	}
}

func (d *Daemon) reconcileWorkerHealthFromPaneMetadata(ctx context.Context, active *ActiveAssignment) {
	if active.Worker.Health == WorkerHealthEscalated {
		return
	}

	metadata, err := d.amux.Metadata(ctx, active.Task.PaneID)
	if err != nil {
		return
	}
	if !strings.EqualFold(strings.TrimSpace(metadata["status"]), WorkerHealthEscalated) {
		return
	}

	active.Worker.Health = WorkerHealthEscalated
	active.Worker.LastSeenAt = d.now()
	_ = d.state.PutWorker(ctx, active.Worker)
}

func (d *Daemon) escalateTaskStartupPaneError(ctx context.Context, active ActiveAssignment, message string, err error) {
	detail := fmt.Sprintf("%s: %v", message, err)
	d.escalateAssignmentError(ctx, active, detail)
}

func (d *Daemon) escalateAssignmentError(ctx context.Context, active ActiveAssignment, message string) {
	if active.Worker.Health == WorkerHealthEscalated {
		return
	}

	now := d.now()
	active.Worker.Health = WorkerHealthEscalated
	active.Worker.LastSeenAt = now
	active.Task.State = TaskStateEscalated
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
		Project:      active.Task.Project,
		Issue:        active.Task.Issue,
		WorkerID:     active.Worker.WorkerID,
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
			Project:      task.Project,
			WorkerID:     task.WorkerID,
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
	d.stopTaskMonitorForProject(active.Task.Project, active.Task.Issue)

	task := active.Task
	task.Status = TaskStatusFailed
	task.State = TaskStateDone
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
		result = errors.Join(result, d.cleanupCloneAndReleaseForProject(cleanupCtx, active.Task.Project, clone, active.Task.Branch))
	}
	result = errors.Join(result, d.state.PutTask(cleanupCtx, task))
	result = errors.Join(result, d.releaseWorkerClaim(cleanupCtx, active.Worker))
	if active.Task.PRNumber > 0 {
		if err := d.state.DeleteMergeEntry(cleanupCtx, active.Task.Project, active.Task.PRNumber); err != nil && !errors.Is(err, ErrTaskNotFound) {
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
		Project:      active.Task.Project,
		Issue:        active.Task.Issue,
		WorkerID:     active.Worker.WorkerID,
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
