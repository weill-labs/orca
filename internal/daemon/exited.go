package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (d *Daemon) handleExitedPaneCapture(ctx context.Context, active ActiveAssignment, profile AgentProfile, snapshot PaneCapture, now time.Time) {
	output := snapshot.Output()
	workerChanged := false
	taskChanged := false

	if output != active.Worker.LastCapture {
		active.Worker.LastCapture = output
		active.Worker.UpdatedAt = now
		active.Task.UpdatedAt = now
		workerChanged = true
		taskChanged = true
	}

	if active.Worker.Health == WorkerHealthEscalated {
		if workerChanged {
			_ = d.state.PutWorker(ctx, active.Worker)
		}
		if taskChanged {
			_ = d.state.PutTask(ctx, active.Task)
		}
		return
	}

	active.Worker.Health = WorkerHealthEscalated
	active.Worker.UpdatedAt = now
	active.Task.UpdatedAt = now
	workerChanged = true
	taskChanged = true

	_ = d.state.PutWorker(ctx, active.Worker)
	_ = d.state.PutTask(ctx, active.Task)

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
		Message:      exitedPaneMessage(snapshot),
	})
}

func exitedPaneMessage(snapshot PaneCapture) string {
	parts := []string{"pane exited"}
	if command := strings.TrimSpace(snapshot.CurrentCommand); command != "" {
		parts = append(parts, fmt.Sprintf("current_command=%s", command))
	}
	parts = append(parts, fmt.Sprintf("child_pids=%v", snapshot.ChildPIDs))
	return strings.Join(parts, " ")
}
