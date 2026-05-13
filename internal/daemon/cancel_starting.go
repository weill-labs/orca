package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

func (d *Daemon) cancelStartingAssignment(ctx context.Context, projectPath string, task Task) error {
	if strings.TrimSpace(task.Project) == "" {
		task.Project = projectPath
	}
	active := d.reconcileActiveAssignment(ctx, task)
	paneID := strings.TrimSpace(firstNonEmpty(active.Task.PaneID, active.Worker.PaneID))
	if paneID != "" {
		active.Task.PaneID = paneID
		exists, _, err := d.paneExists(ctx, paneID)
		if err != nil {
			return fmt.Errorf("check pane %s: %w", paneID, err)
		}
		if exists {
			killErr := ignorePaneAlreadyGoneError(d.amux.KillPane(d.cleanupContext(ctx), paneID))
			clearErr := d.clearStartingAssignment(ctx, active, "starting task cancelled and cleared")
			return errors.Join(killErr, clearErr)
		}
	}

	return d.clearStartingAssignment(ctx, active, "starting task cancelled and cleared")
}

func (d *Daemon) clearStartingAssignment(ctx context.Context, active ActiveAssignment, message string) error {
	var result error
	cleanupCtx := d.cleanupContext(ctx)
	projectPath := d.projectPathForTask(active.Task)
	active.Task.Project = projectPath
	if strings.TrimSpace(active.Worker.Project) == "" {
		active.Worker.Project = projectPath
	}
	d.stopTaskMonitorForProject(projectPath, active.Task.Issue)

	clone := Clone{
		Name: active.Task.CloneName,
		Path: active.Task.ClonePath,
	}
	if clone.Name == "" && clone.Path != "" {
		clone.Name = filepath.Base(clone.Path)
	}
	if strings.TrimSpace(clone.Path) != "" {
		result = errors.Join(result, d.cleanupCloneAndReleaseForProject(cleanupCtx, projectPath, clone, active.Task.Branch))
	}
	result = errors.Join(result, d.releaseWorkerClaim(cleanupCtx, active.Worker))
	if err := d.state.DeleteTask(cleanupCtx, projectPath, active.Task.Issue); err != nil && !errors.Is(err, ErrTaskNotFound) {
		result = errors.Join(result, err)
	}
	if active.Task.PRNumber > 0 {
		if err := d.state.DeleteMergeEntry(cleanupCtx, projectPath, active.Task.PRNumber); err != nil && !errors.Is(err, ErrTaskNotFound) {
			result = errors.Join(result, err)
		}
	}
	d.requestRelayReconnect()

	if message == "" {
		message = "starting task cancelled and cleared"
	}
	profile, err := d.profileForTask(cleanupCtx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}
	d.emit(cleanupCtx, Event{
		Time:         d.now(),
		Type:         EventTaskCancelled,
		Project:      projectPath,
		Issue:        active.Task.Issue,
		WorkerID:     active.Worker.WorkerID,
		PaneID:       active.Task.PaneID,
		PaneName:     assignmentPaneName(active.Task, active.Worker),
		CloneName:    clone.Name,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      message,
	})
	return result
}
