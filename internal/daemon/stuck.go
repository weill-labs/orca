package daemon

import "strings"

func (d *Daemon) nudgeOrEscalate(active *assignment, reason string) {
	now := d.now()
	active.worker.Health = WorkerHealthStuck
	active.worker.UpdatedAt = now

	if active.worker.NudgeCount < active.profile.MaxNudgeRetries {
		if err := d.amux.SendKeys(active.ctx, active.pane.ID, active.profile.NudgeCommand); err != nil {
			return
		}
		active.worker.NudgeCount++
		_ = d.state.PutWorker(active.ctx, active.worker)
		d.emit(active.ctx, Event{
			Time:         now,
			Type:         EventWorkerNudged,
			Project:      d.project,
			Issue:        active.task.Issue,
			PaneID:       active.pane.ID,
			PaneName:     active.pane.Name,
			CloneName:    active.clone.Name,
			ClonePath:    active.clone.Path,
			Branch:       active.task.Branch,
			AgentProfile: active.profile.Name,
			Retry:        active.worker.NudgeCount,
			Message:      reason,
		})
		return
	}

	if active.escalated {
		return
	}
	active.escalated = true
	_ = d.state.PutWorker(active.ctx, active.worker)
	d.emit(active.ctx, Event{
		Time:         now,
		Type:         EventWorkerEscalated,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		Retry:        active.worker.NudgeCount,
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
