package daemon

import (
	"context"
	"fmt"
	"strings"
)

func (d *Daemon) nudgeOrEscalate(ctx context.Context, active ActiveAssignment, profile AgentProfile, reason string) {
	now := d.now()
	previousHealth := active.Worker.Health
	active.Worker.Health = WorkerHealthStuck
	active.Worker.UpdatedAt = now

	if active.Worker.NudgeCount < profile.MaxNudgeRetries {
		if err := d.amux.SendKeys(ctx, active.Task.PaneID, profile.NudgeCommand); err != nil {
			return
		}
		active.Worker.NudgeCount++
		_ = d.state.PutWorker(ctx, active.Worker)
		d.emit(ctx, Event{
			Time:         now,
			Type:         EventWorkerNudged,
			Project:      d.project,
			Issue:        active.Task.Issue,
			PaneID:       active.Task.PaneID,
			PaneName:     active.Task.PaneName,
			CloneName:    active.Task.CloneName,
			ClonePath:    active.Task.ClonePath,
			Branch:       active.Task.Branch,
			AgentProfile: profile.Name,
			Retry:        active.Worker.NudgeCount,
			Message:      reason,
		})
		return
	}

	if previousHealth == WorkerHealthEscalated {
		return
	}
	active.Worker.Health = WorkerHealthEscalated
	_ = d.state.PutWorker(ctx, active.Worker)

	message := reason
	logPath, diagnosticsErr := d.captureStuckWorkerDiagnostics(context.WithoutCancel(ctx), active, profile, reason)
	if logPath != "" {
		message = fmt.Sprintf("%s; diagnostics saved to %s", message, logPath)
	}
	if diagnosticsErr != nil {
		message = fmt.Sprintf("%s; diagnostics error: %v", message, diagnosticsErr)
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

func (d *Daemon) matchesStuckPattern(profile AgentProfile, output string) bool {
	lower := strings.ToLower(output)
	for _, pattern := range profile.StuckTextPatterns {
		if strings.Contains(lower, strings.ToLower(pattern)) {
			return true
		}
	}
	return false
}
