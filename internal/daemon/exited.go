package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	exitedPaneEscalationWindowMultiplier = 6
	minimumExitedPaneEscalationWindow    = 30 * time.Second
)

func (d *Daemon) exitedPaneEscalationWindow() time.Duration {
	window := exitedPaneEscalationWindowMultiplier * d.captureInterval
	if window < minimumExitedPaneEscalationWindow {
		return minimumExitedPaneEscalationWindow
	}
	return window
}

func (d *Daemon) shouldEscalateExitedPane(snapshot PaneCapture, now time.Time) bool {
	exitedAt, ok := snapshot.ExitedAt()
	if !ok {
		return true
	}
	if exitedAt.After(now) {
		return false
	}
	return now.Sub(exitedAt) >= d.exitedPaneEscalationWindow()
}

func (d *Daemon) handleExitedPaneCapture(ctx context.Context, active ActiveAssignment, profile AgentProfile, snapshot PaneCapture, now time.Time) {
	d.applyTaskStateUpdate(ctx, d.exitedPaneStateUpdate(active, profile, snapshot, now, true))
}

func (d *Daemon) checkTaskExitedEvent(ctx context.Context, active ActiveAssignment) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}

	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return update
	}

	snapshot, err := d.amux.CapturePane(ctx, active.Task.PaneID)
	if err != nil {
		snapshot = PaneCapture{}
	}
	snapshot.Exited = true

	return d.exitedPaneStateUpdate(active, profile, snapshot, d.now(), true)
}

func (d *Daemon) exitedPaneStateUpdate(active ActiveAssignment, profile AgentProfile, snapshot PaneCapture, now time.Time, force bool) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}
	output := snapshot.Output()

	if shouldUpdateExitedCapture(output, update.Active.Worker.LastCapture) {
		update.Active.Worker.LastCapture = output
		update.Active.Worker.LastSeenAt = now
		update.Active.Task.UpdatedAt = now
		update.WorkerChanged = true
		update.TaskChanged = true
	}

	if update.Active.Worker.Health == WorkerHealthEscalated {
		return update
	}
	if !force && !d.shouldEscalateExitedPane(snapshot, now) {
		return update
	}

	update.Active.Worker.Health = WorkerHealthEscalated
	update.Active.Worker.LastSeenAt = now
	update.Active.Task.UpdatedAt = now
	update.WorkerChanged = true
	update.TaskChanged = true
	update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerEscalated, exitedPaneMessage(snapshot)))

	return update
}

func shouldUpdateExitedCapture(next, current string) bool {
	if next == "" && current != "" {
		return false
	}
	return next != current
}

func exitedPaneMessage(snapshot PaneCapture) string {
	parts := []string{"pane exited"}
	if command := strings.TrimSpace(snapshot.CurrentCommand); command != "" {
		parts = append(parts, fmt.Sprintf("current_command=%s", command))
	}
	parts = append(parts, fmt.Sprintf("child_pids=%v", snapshot.ChildPIDs))
	return strings.Join(parts, " ")
}
