package daemon

import (
	"context"
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
	if active.Task.PaneID != "" {
		_ = d.setPaneMetadata(ctx, active.Task.PaneID, map[string]string{"status": "escalated"})
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
		Message:      reason,
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
