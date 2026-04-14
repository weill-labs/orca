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
	exitedPaneRestartWindow              = 5 * time.Minute
	secondExitedPaneRestartDelay         = 10 * time.Second
	thirdExitedPaneRestartDelay          = 60 * time.Second
	maxExitedPaneRestartCount            = 3
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

func (d *Daemon) maybeResetExitedPaneRestartWindow(update *TaskStateUpdate, now time.Time) {
	if update.Active.Worker.Health != WorkerHealthHealthy {
		return
	}
	if update.Active.Worker.FirstCrashAt.IsZero() || update.Active.Worker.FirstCrashAt.After(now) {
		return
	}
	if now.Sub(update.Active.Worker.FirstCrashAt) <= exitedPaneRestartWindow {
		return
	}

	resetExitedPaneRestartWindow(&update.Active.Worker)
	update.Active.Worker.LastSeenAt = now
	update.WorkerChanged = true
}

func resetExitedPaneRestartWindow(worker *Worker) {
	if worker == nil {
		return
	}
	worker.RestartCount = 0
	worker.FirstCrashAt = time.Time{}
}

func exitedPaneRestartPlan(worker Worker, now time.Time) (delay time.Duration, restartCount int, firstCrashAt time.Time, escalate bool) {
	restartCount = worker.RestartCount
	firstCrashAt = worker.FirstCrashAt
	if firstCrashAt.IsZero() || firstCrashAt.After(now) || now.Sub(firstCrashAt) > exitedPaneRestartWindow {
		restartCount = 0
		firstCrashAt = now
	}
	if restartCount >= maxExitedPaneRestartCount {
		return 0, restartCount, firstCrashAt, true
	}

	switch restartCount + 1 {
	case 1:
		return 0, 1, firstCrashAt, false
	case 2:
		return secondExitedPaneRestartDelay, 2, firstCrashAt, false
	case 3:
		return thirdExitedPaneRestartDelay, 3, firstCrashAt, false
	}
	// Unreachable: restartCount >= maxExitedPaneRestartCount returns above,
	// and the reset window bounds the remaining cases to 0..2.
	return 0, restartCount, firstCrashAt, true
}

func (d *Daemon) planExitedPaneRecovery(update *TaskStateUpdate, profile AgentProfile, snapshot PaneCapture, now time.Time) {
	delay, restartCount, firstCrashAt, escalate := exitedPaneRestartPlan(update.Active.Worker, now)
	if escalate {
		update.Active.Worker.Health = WorkerHealthEscalated
		update.Active.Worker.LastSeenAt = now
		update.Active.Task.UpdatedAt = now
		update.WorkerChanged = true
		update.TaskChanged = true
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerEscalated, exitedPaneMessage(snapshot)))
		return
	}

	update.queueNudge(func(ctx context.Context, d *Daemon, update *TaskStateUpdate) {
		if delay > 0 {
			if err := d.sleep(ctx, delay); err != nil {
				return
			}
		}

		startupSnapshot, err := d.startAgentInPane(ctx, update.Active.Task.PaneID, profile)
		if err != nil {
			update.Active.Worker.RestartCount = restartCount
			update.Active.Worker.FirstCrashAt = firstCrashAt
			update.Active.Worker.Health = WorkerHealthEscalated
			update.Active.Worker.LastSeenAt = now
			update.Active.Task.UpdatedAt = now
			update.WorkerChanged = true
			update.TaskChanged = true
			update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerEscalated, fmt.Sprintf("%s auto restart failed: %v", exitedPaneMessage(snapshot), err)))
			return
		}

		update.Active.Worker.RestartCount = restartCount
		update.Active.Worker.FirstCrashAt = firstCrashAt
		update.Active.Worker.LastCapture = startupSnapshot.Output()
		update.Active.Worker.Health = WorkerHealthHealthy
		update.Active.Worker.NudgeCount = 0
		update.Active.Worker.LastActivityAt = now
		update.Active.Worker.LastSeenAt = now
		update.Active.Task.UpdatedAt = now
		update.WorkerChanged = true
		update.TaskChanged = true
	})
}

func (d *Daemon) handleExitedPaneCapture(ctx context.Context, active ActiveAssignment, profile AgentProfile, snapshot PaneCapture, now time.Time) {
	update := d.exitedPaneStateUpdate(active, profile, snapshot, now, false)
	update.runNudges(ctx, d)
	d.applyTaskStateUpdate(ctx, update)
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

	d.planExitedPaneRecovery(&update, profile, snapshot, now)
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
