package daemon

import (
	"context"
	"strings"
	"time"
)

func (d *Daemon) nudgeOrEscalate(ctx context.Context, update *TaskStateUpdate, profile AgentProfile, reason string, now time.Time) {
	previousHealth := update.Active.Worker.Health

	if update.Active.Worker.NudgeCount < profile.MaxNudgeRetries {
		update.queueNudge(func(ctx context.Context, d *Daemon, update *TaskStateUpdate) {
			if err := d.amuxClient(ctx).SendKeys(ctx, update.Active.Task.PaneID, profile.NudgeCommand); err != nil {
				if isPaneGoneError(err) {
					d.escalateTaskState(update, profile, "worker pane missing during stuck nudge", now)
				}
				return
			}

			update.Active.Worker.Health = WorkerHealthStuck
			update.Active.Worker.LastSeenAt = now
			update.Active.Worker.NudgeCount++
			update.WorkerChanged = true

			event := d.assignmentEvent(update.Active, profile, EventWorkerNudged, reason)
			event.Retry = update.Active.Worker.NudgeCount
			update.Events = append(update.Events, event)
		})
		return
	}

	if previousHealth == WorkerHealthEscalated {
		return
	}

	d.escalateTaskState(update, profile, reason, now)
	if update.Active.Task.PaneID != "" {
		update.PaneMetadata = mergeMetadata(update.PaneMetadata, map[string]string{"status": "escalated"})
	}
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
