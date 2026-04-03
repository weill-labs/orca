package daemon

import "context"

func (d *Daemon) runLoop(ctx context.Context, done chan struct{}) {
	defer close(done)

	captureTick := d.newTicker(d.captureInterval)
	defer captureTick.Stop()
	pollTick := d.newTicker(d.pollInterval)
	defer pollTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-captureTick.C():
			d.runCaptureTick(ctx)
		case <-pollTick.C():
			d.runPollTick(ctx)
		}
	}
}

func (d *Daemon) runCaptureTick(ctx context.Context) {
	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err != nil {
		return
	}

	for _, active := range assignments {
		if ctx.Err() != nil {
			return
		}
		d.handleCapture(ctx, active)
	}
}

func (d *Daemon) runPollTick(ctx context.Context) {
	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err == nil {
		for _, active := range assignments {
			if ctx.Err() != nil {
				return
			}
			d.handlePRPoll(ctx, active)
		}
	}

	d.processMergeQueue(ctx)
}

func (d *Daemon) handleCapture(ctx context.Context, active ActiveAssignment) {
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return
	}

	output, err := d.amux.Capture(ctx, active.Task.PaneID)
	if err != nil {
		return
	}

	now := d.now()
	changed := output != active.Worker.LastCapture
	if changed {
		wasStuck := active.Worker.Health == WorkerHealthStuck ||
			active.Worker.Health == WorkerHealthEscalated ||
			active.Worker.NudgeCount > 0
		active.Worker.LastCapture = output
		active.Worker.LastActivityAt = now
		active.Worker.Health = WorkerHealthHealthy
		active.Worker.NudgeCount = 0
		active.Worker.UpdatedAt = now
		active.Task.UpdatedAt = now
		_ = d.state.PutWorker(ctx, active.Worker)
		_ = d.state.PutTask(ctx, active.Task)

		if wasStuck {
			d.emit(ctx, Event{
				Time:         now,
				Type:         EventWorkerRecovered,
				Project:      d.project,
				Issue:        active.Task.Issue,
				PaneID:       active.Task.PaneID,
				PaneName:     active.Task.PaneName,
				CloneName:    active.Task.CloneName,
				ClonePath:    active.Task.ClonePath,
				Branch:       active.Task.Branch,
				AgentProfile: profile.Name,
				Message:      "worker output changed",
			})
		}
	}

	if d.matchesStuckPattern(profile, output) {
		d.nudgeOrEscalate(ctx, active, profile, "matched stuck text pattern")
		return
	}
	if profile.StuckTimeout > 0 && now.Sub(active.Worker.LastActivityAt) >= profile.StuckTimeout {
		d.nudgeOrEscalate(ctx, active, profile, "idle timeout exceeded")
	}
}
