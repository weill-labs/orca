package daemon

import (
	"context"
	"strings"
	"time"
)

const (
	adaptivePRFastPollInterval = 5 * time.Second
	adaptivePRWarmPollInterval = 15 * time.Second
	adaptivePRSlowPollInterval = 30 * time.Second
	openPRPollIntervalCap      = 2 * time.Minute
	adaptivePRFastPollWindow   = 10 * time.Minute
	adaptivePRWarmPollWindow   = 30 * time.Minute
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
	pollInterval := d.currentPRPollInterval()
	pollTick := d.newTicker(prPollSchedulerTickInterval(pollInterval))
	defer func() {
		pollTick.Stop()
	}()
	captureTickCh := captureTick.C()
	pollTickCh := pollTick.C()
	pollIntervalCh := d.pollIntervalCh
	captureInFlight := false
	pollInFlight := false
	var lastMissingPRNumberReconcile time.Time

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
		case interval := <-pollIntervalCh:
			if interval <= 0 || interval == pollInterval {
				continue
			}
			pollTick.Stop()
			drainMonitorTicks(pollTickCh)
			pollInterval = interval
			pollTick = d.newTicker(prPollSchedulerTickInterval(pollInterval))
			pollTickCh = pollTick.C()
		case <-pollTickCh:
			if pollInFlight {
				continue
			}
			pollInFlight = true
			d.runPollTick(ctx)
			now := d.now()
			if shouldRunMissingPRNumberReconciliation(lastMissingPRNumberReconcile, now) {
				d.reconcileMissingPRNumbers(ctx)
				lastMissingPRNumberReconcile = now
			}
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
		assignments = dueAssignmentsForPRPoll(d.now(), assignments, d.currentPRPollInterval())
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

		d.escalateAssignmentError(ctx, active, "worker pane missing during monitor reconciliation")
	}
	return reconciled
}

func prPollSchedulerTickInterval(defaultInterval time.Duration) time.Duration {
	if defaultInterval <= 0 {
		return adaptivePRFastPollInterval
	}
	if defaultInterval > adaptivePRSlowPollInterval {
		return defaultInterval
	}
	if defaultInterval < adaptivePRFastPollInterval {
		return defaultInterval
	}
	return adaptivePRFastPollInterval
}

func dueAssignmentsForPRPoll(now time.Time, assignments []ActiveAssignment, defaultInterval time.Duration) []ActiveAssignment {
	due := make([]ActiveAssignment, 0, len(assignments))
	for _, active := range assignments {
		if shouldPollAssignmentForPR(now, active, defaultInterval) {
			due = append(due, active)
		}
	}
	return due
}

func shouldPollAssignmentForPR(now time.Time, active ActiveAssignment, defaultInterval time.Duration) bool {
	if active.Task.PRNumber != active.Worker.LastPRNumber {
		return true
	}
	if active.Worker.LastPRPollAt.IsZero() {
		return true
	}
	interval := adaptivePRPollInterval(now, active.Worker, defaultInterval)
	if interval <= 0 {
		return true
	}
	return !now.Before(active.Worker.LastPRPollAt.Add(interval))
}

func adaptivePRPollInterval(now time.Time, worker Worker, defaultInterval time.Duration) time.Duration {
	if defaultInterval <= 0 {
		defaultInterval = adaptivePRSlowPollInterval
	}
	if worker.LastPushAt.IsZero() {
		return defaultInterval
	}

	elapsed := now.Sub(worker.LastPushAt)
	switch {
	case elapsed < 0:
		return adaptivePRFastPollInterval
	case elapsed < adaptivePRFastPollWindow:
		return adaptivePRFastPollInterval
	case elapsed < adaptivePRWarmPollWindow:
		return adaptivePRWarmPollInterval
	default:
		return adaptivePRSlowPollInterval
	}
}

func syncWorkerPRTracking(now time.Time, active *ActiveAssignment) bool {
	changed := false
	if active.Worker.LastPRNumber != active.Task.PRNumber {
		active.Worker.LastPRNumber = active.Task.PRNumber
		if active.Task.PRNumber > 0 {
			active.Worker.LastPushAt = now
		} else {
			active.Worker.LastPushAt = time.Time{}
		}
		changed = true
	}

	switch {
	case active.Task.PRNumber > 0 && active.Worker.LastPushAt.IsZero():
		active.Worker.LastPushAt = now
		changed = true
	case active.Task.PRNumber == 0 && !active.Worker.LastPushAt.IsZero():
		active.Worker.LastPushAt = time.Time{}
		changed = true
	}

	return changed
}
