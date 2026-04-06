package daemon

import (
	"context"
	"strings"
	"time"
)

func (d *Daemon) nudgeOrEscalate(ctx context.Context, update *TaskStateUpdate, profile AgentProfile, reason string, now time.Time) {
	previousHealth := update.Active.Worker.Health

	if update.Active.Worker.NudgeCount < profile.MaxNudgeRetries {
		if err := d.amux.SendKeys(ctx, update.Active.Task.PaneID, profile.NudgeCommand); err != nil {
			return
		}

		update.Active.Worker.Health = WorkerHealthStuck
		update.Active.Worker.UpdatedAt = now
		update.Active.Worker.NudgeCount++
		update.WorkerChanged = true

		event := d.assignmentEvent(update.Active, profile, EventWorkerNudged, reason)
		event.Retry = update.Active.Worker.NudgeCount
		update.Events = append(update.Events, event)
		return
	}

	if previousHealth == WorkerHealthEscalated {
		return
	}

	update.Active.Worker.Health = WorkerHealthEscalated
	update.Active.Worker.UpdatedAt = now
	update.WorkerChanged = true
	if update.Active.Task.PaneID != "" {
		update.PaneMetadata = mergeMetadata(update.PaneMetadata, map[string]string{"status": "escalated"})
	}

	event := d.assignmentEvent(update.Active, profile, EventWorkerEscalated, reason)
	event.Retry = update.Active.Worker.NudgeCount
	update.Events = append(update.Events, event)
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
