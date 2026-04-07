package daemon

import (
	"context"
	"time"
)

type monitorTickKind int

const (
	monitorTickCapture monitorTickKind = iota
	monitorTickPoll
)

func (d *Daemon) runLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	defer d.waitForMonitorRuns()
	defer d.stopAllTaskMonitors(true)

	captureTick := d.newTicker(d.captureInterval)
	defer captureTick.Stop()
	pollTick := d.newTicker(d.pollInterval)
	defer pollTick.Stop()
	captureTickCh := captureTick.C()
	pollTickCh := pollTick.C()
	monitorDone := make(chan monitorTickKind, 2)
	captureInFlight := false
	pollInFlight := false

	for {
		select {
		case <-ctx.Done():
			return
		case kind := <-monitorDone:
			switch kind {
			case monitorTickCapture:
				drainMonitorTicks(captureTickCh)
				captureInFlight = false
			case monitorTickPoll:
				drainMonitorTicks(pollTickCh)
				pollInFlight = false
			}
		case update := <-d.mergeQueueUpdates:
			d.applyMergeQueueUpdate(ctx, update)
		case <-captureTickCh:
			if captureInFlight {
				continue
			}
			captureInFlight = true
			d.startMonitorTick(monitorDone, monitorTickCapture, func() {
				d.runCaptureTick(ctx)
			})
		case <-pollTickCh:
			if pollInFlight {
				continue
			}
			pollInFlight = true
			d.startMonitorTick(monitorDone, monitorTickPoll, func() {
				d.runPollTick(ctx)
			})
		}
	}
}

func (d *Daemon) startMonitorTick(done chan<- monitorTickKind, kind monitorTickKind, run func()) {
	d.monitorRuns.Add(1)
	go func() {
		defer d.monitorRuns.Done()
		run()
		done <- kind
	}()
}

func (d *Daemon) waitForMonitorRuns() {
	d.monitorRuns.Wait()
}

func drainMonitorTicks(tickCh <-chan time.Time) {
	for {
		select {
		case <-tickCh:
		default:
			return
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
	d.applyMergeQueueUpdates(ctx)

	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err == nil {
		results := d.dispatchTaskMonitorChecks(ctx, assignments, taskMonitorCheckPRPoll)
		d.applyTaskMonitorResults(ctx, results)
	}

	d.dispatchMergeQueue(ctx)
	d.applyMergeQueueUpdates(ctx)
}
