package daemon

import (
	"context"
	"strings"
	"time"

	"github.com/weill-labs/orca/internal/amux"
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
	pollSchedulerInterval := prPollSchedulerTickInterval(pollInterval)
	pollTick := d.newTicker(pollSchedulerInterval)
	defer func() {
		pollTick.Stop()
	}()
	captureTickCh := captureTick.C()
	pollTickCh := pollTick.C()
	pollIntervalCh := d.pollIntervalCh
	captureInFlight := false
	pollInFlight := false
	var lastPollTickFinishedAt time.Time
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
			timing := d.runCaptureTick(ctx)
			timing.log(d.logf)
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
			pollSchedulerInterval = prPollSchedulerTickInterval(pollInterval)
			pollTick = d.newTicker(pollSchedulerInterval)
			pollTickCh = pollTick.C()
		case <-pollTickCh:
			pollTickStartedAt := d.now()
			d.logPollBetweenTicks(lastPollTickFinishedAt, pollTickStartedAt, pollSchedulerInterval)
			if pollInFlight {
				continue
			}
			pollInFlight = true
			timing := d.runPollTick(ctx)
			now := d.now()
			if shouldRunMissingPRNumberReconciliation(lastMissingPRNumberReconcile, now) {
				done := timing.stage("missing_pr_reconcile")
				d.reconcileMissingPRNumbers(ctx)
				done()
				lastMissingPRNumberReconcile = now
			}
			timing.log(d.logf)
			d.recordHeartbeat(ctx)
			drainMonitorTicks(pollTickCh)
			lastPollTickFinishedAt = d.now()
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

func (d *Daemon) runCaptureTick(ctx context.Context) *pollTickTiming {
	timing := newPollTickTiming("capture", d.now)
	ctx = withPollTickTiming(ctx, timing)
	ctx = d.withMonitorCircuits(ctx)
	done := timing.stage("state_read")
	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	done()
	if err != nil {
		return timing
	}
	done = timing.stage("pane_reconcile")
	assignments = d.reconcileTrackedPanes(ctx, assignments)
	done()
	done = timing.stage("monitor_checks")
	results := d.dispatchTaskMonitorChecks(ctx, assignments, taskMonitorCheckCapture)
	done()
	done = timing.stage("apply_results")
	d.applyTaskMonitorResults(ctx, results)
	done()
	return timing
}

func (d *Daemon) runPollTick(ctx context.Context) *pollTickTiming {
	timing := newPollTickTiming("poll", d.now)
	ctx = withPollTickTiming(ctx, timing)
	ctx = d.withMonitorCircuits(ctx)
	done := timing.stage("merge_updates_initial")
	d.applyMergeQueueUpdates(ctx)
	done()

	done = timing.stage("state_read")
	tasks, err := d.prPollTasks(ctx)
	done()
	if err == nil {
		done = timing.stage("resume_workers")
		assignments := d.prPollAssignmentsForTasks(ctx, tasks)
		done()
		done = timing.stage("pane_reconcile")
		assignments = d.reconcileTrackedPanes(ctx, assignments)
		done()
		done = timing.stage("schedule_filter")
		assignments = dueAssignmentsForPRPoll(d.now(), assignments, d.currentPRPollInterval())
		done()
		done = timing.stage("monitor_checks")
		results := d.dispatchTaskMonitorChecks(ctx, assignments, taskMonitorCheckPRPoll)
		done()
		done = timing.stage("apply_results")
		d.applyTaskMonitorResults(ctx, results)
		done()
	}

	done = timing.stage("dispatch_merge_queue")
	d.dispatchMergeQueue(ctx)
	done()
	done = timing.stage("merge_updates_final")
	d.applyMergeQueueUpdates(ctx)
	done()
	return timing
}

func (d *Daemon) prPollAssignments(ctx context.Context) ([]ActiveAssignment, error) {
	tasks, err := d.prPollTasks(ctx)
	if err != nil {
		return nil, err
	}
	return d.prPollAssignmentsForTasks(ctx, tasks), nil
}

func (d *Daemon) prPollTasks(ctx context.Context) ([]Task, error) {
	tasks, err := d.state.NonTerminalTasks(ctx, d.project)
	if err != nil {
		d.emitProjectPRPollTrace(ctx, d.project, "list_non_terminal_tasks_error", err)
		return nil, err
	}
	return tasks, nil
}

func (d *Daemon) prPollAssignmentsForTasks(ctx context.Context, tasks []Task) []ActiveAssignment {
	assignments := make([]ActiveAssignment, 0, len(tasks))
	for _, task := range tasks {
		worker, hasWorker, err := d.resumeWorker(ctx, task)
		taskWorker := Worker{
			Project:      task.Project,
			WorkerID:     task.WorkerID,
			PaneID:       task.PaneID,
			PaneName:     task.PaneName,
			Issue:        task.Issue,
			ClonePath:    task.ClonePath,
			AgentProfile: task.AgentProfile,
		}
		if err != nil {
			d.emitPRPollTaskTrace(ctx, task, taskWorker, "resume_worker_error", err)
			continue
		}
		if !hasWorker {
			d.emitPRPollTaskTrace(ctx, task, taskWorker, "resume_worker_missing", nil)
			continue
		}
		assignments = append(assignments, ActiveAssignment{
			Task:   task,
			Worker: worker,
		})
	}
	return assignments
}

func (d *Daemon) reconcileTrackedPanes(ctx context.Context, assignments []ActiveAssignment) []ActiveAssignment {
	reconciled := make([]ActiveAssignment, 0, len(assignments))
	for _, active := range assignments {
		paneID := strings.TrimSpace(active.Task.PaneID)
		if paneID == "" {
			reconciled = append(reconciled, active)
			continue
		}

		exists, notFound, err := d.paneExists(ctx, paneID)
		if err != nil {
			d.emitPRPollTaskTrace(ctx, active.Task, active.Worker, "pane_exists_error", err)
			reconciled = append(reconciled, active)
			continue
		}
		if exists {
			reconciled = append(reconciled, active)
			continue
		}
		if notFound && active.Worker.Health != WorkerHealthEscalated {
			d.emitPRPollTaskTrace(ctx, active.Task, active.Worker, "pane_exists_error", amux.ErrPaneNotFound)
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
