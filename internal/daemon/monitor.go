package daemon

import "context"

func (d *Daemon) runLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	defer d.stopAllTaskMonitors(true)

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
	results := d.dispatchTaskMonitorChecks(ctx, assignments, taskMonitorCheckCapture)
	d.applyTaskMonitorResults(ctx, results)
}

func (d *Daemon) runPollTick(ctx context.Context) {
	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err == nil {
		results := d.dispatchTaskMonitorChecks(ctx, assignments, taskMonitorCheckPRPoll)
		d.applyTaskMonitorResults(ctx, results)
	}

	d.processMergeQueue(ctx)
}
