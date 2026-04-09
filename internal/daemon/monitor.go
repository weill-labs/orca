package daemon

import (
	"context"
	"strings"
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
	captureInFlight := false
	pollInFlight := false

	for {
		select {
		case <-ctx.Done():
			return
		case update := <-d.mergeQueueUpdates:
			d.applyMergeQueueUpdate(ctx, update)
		case <-captureTickCh:
			if captureInFlight {
				continue
			}
			captureInFlight = true
			d.runCaptureTick(ctx)
			d.recordHeartbeat(ctx)
			drainMonitorTicks(captureTickCh)
			captureInFlight = false
		case <-pollTickCh:
			if pollInFlight {
				continue
			}
			pollInFlight = true
			d.runPollTick(ctx)
			d.recordHeartbeat(ctx)
			drainMonitorTicks(pollTickCh)
			pollInFlight = false
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
	ctx = d.withMonitorCircuits(ctx)
	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err != nil {
		return
	}
	assignments = d.reconcileTrackedPanes(ctx, assignments)
	results := d.dispatchTaskMonitorChecks(ctx, assignments, taskMonitorCheckCapture)
	d.applyTaskMonitorResults(ctx, results)
}

func (d *Daemon) runPollTick(ctx context.Context) {
	ctx = d.withMonitorCircuits(ctx)
	d.applyMergeQueueUpdates(ctx)

	assignments, err := d.prPollAssignments(ctx)
	if err == nil {
		assignments = d.reconcileTrackedPanes(ctx, assignments)
		results := d.dispatchTaskMonitorChecks(ctx, assignments, taskMonitorCheckPRPoll)
		d.applyTaskMonitorResults(ctx, results)
	}

	d.dispatchMergeQueue(ctx)
	d.applyMergeQueueUpdates(ctx)
}

func (d *Daemon) prPollAssignments(ctx context.Context) ([]ActiveAssignment, error) {
	tasks, err := d.state.NonTerminalTasks(ctx, d.project)
	if err != nil {
		return nil, err
	}

	assignments := make([]ActiveAssignment, 0, len(tasks))
	for _, task := range tasks {
		worker, hasWorker, err := d.resumeWorker(ctx, task)
		if err != nil || !hasWorker {
			continue
		}
		assignments = append(assignments, ActiveAssignment{
			Task:   task,
			Worker: worker,
		})
	}
	return assignments, nil
}

func (d *Daemon) reconcileTrackedPanes(ctx context.Context, assignments []ActiveAssignment) []ActiveAssignment {
	reconciled := make([]ActiveAssignment, 0, len(assignments))
	for _, active := range assignments {
		paneID := strings.TrimSpace(active.Task.PaneID)
		if paneID == "" {
			reconciled = append(reconciled, active)
			continue
		}

		exists, err := d.amux.PaneExists(ctx, paneID)
		if err != nil || exists {
			reconciled = append(reconciled, active)
			continue
		}

		d.escalateAssignmentOnStartupError(ctx, active, "worker pane missing during monitor reconciliation")
	}
	return reconciled
}
