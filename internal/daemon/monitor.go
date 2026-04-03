package daemon

func (d *Daemon) monitorAssignment(active *assignment) {
	defer d.wg.Done()
	defer active.captureTick.Stop()
	defer active.pollTick.Stop()

	for {
		select {
		case <-active.ctx.Done():
			return
		case <-active.captureTick.C():
			d.handleCapture(active)
		case <-active.pollTick.C():
			d.handlePRPoll(active)
		}
	}
}

func (d *Daemon) handleCapture(active *assignment) {
	output, err := d.amux.Capture(active.ctx, active.pane.ID)
	if err != nil {
		return
	}

	now := d.now()
	changed := output != active.lastOutput
	if changed {
		wasStuck := active.worker.Health == WorkerHealthStuck || active.worker.NudgeCount > 0 || active.escalated
		active.lastOutput = output
		active.lastActivity = now
		active.escalated = false
		active.worker.Health = WorkerHealthHealthy
		active.worker.NudgeCount = 0
		active.worker.UpdatedAt = now
		active.task.UpdatedAt = now
		_ = d.state.PutWorker(active.ctx, active.worker)
		_ = d.state.PutTask(active.ctx, active.task)
		if wasStuck {
			d.emit(active.ctx, Event{
				Time:         now,
				Type:         EventWorkerRecovered,
				Project:      d.project,
				Issue:        active.task.Issue,
				PaneID:       active.pane.ID,
				PaneName:     active.pane.Name,
				CloneName:    active.clone.Name,
				ClonePath:    active.clone.Path,
				Branch:       active.task.Branch,
				AgentProfile: active.profile.Name,
				Message:      "worker output changed",
			})
		}
	}

	if d.matchesStuckPattern(active.profile, output) {
		d.nudgeOrEscalate(active, "matched stuck text pattern")
		return
	}
	if active.profile.StuckTimeout > 0 && now.Sub(active.lastActivity) >= active.profile.StuckTimeout {
		d.nudgeOrEscalate(active, "idle timeout exceeded")
	}
}
