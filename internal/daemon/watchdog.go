package daemon

import (
	"context"
	"time"
)

const (
	// Warn after two missed capture intervals (10s by default): one interval is
	// the expected heartbeat cadence, and the second gives normal tick jitter a
	// full interval of slack while still logging diagnostics before unhealthy.
	watchdogWarningMultiplier   = 2
	watchdogUnhealthyMultiplier = 5
)

type daemonStatusWriter interface {
	Update(ctx context.Context, status string, heartbeatAt time.Time) error
}

type heartbeatStatusUpdate struct {
	status      string
	heartbeatAt time.Time
}

func (d *Daemon) runWatchdog(ctx context.Context, done chan struct{}) {
	defer close(done)

	ticker := d.newWatchdogTicker(d.captureInterval)
	defer ticker.Stop()

	warned := false
	unhealthy := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			heartbeatAt := d.lastHeartbeatTime()
			if heartbeatAt.IsZero() {
				continue
			}

			staleFor := d.now().Sub(heartbeatAt)
			if staleFor <= watchdogWarningMultiplier*d.captureInterval {
				warned = false
				unhealthy = false
				continue
			}

			if !warned {
				d.logWatchdogWarning(staleFor, heartbeatAt)
				warned = true
			}
			if staleFor <= watchdogUnhealthyMultiplier*d.captureInterval || unhealthy {
				continue
			}

			d.updateDaemonStatus(ctx, daemonStatusUnhealthy, heartbeatAt)
			unhealthy = true
		}
	}
}

func (d *Daemon) recordHeartbeat(ctx context.Context) {
	heartbeatAt := d.now()
	d.lastHeartbeat.Store(heartbeatAt.UnixMilli())
	d.updateDaemonStatus(ctx, daemonStatusRunning, heartbeatAt)
}

func (d *Daemon) updateDaemonStatus(ctx context.Context, status string, heartbeatAt time.Time) {
	if d.statusWriter == nil {
		return
	}
	update := heartbeatStatusUpdate{status: status, heartbeatAt: heartbeatAt}
	if d.statusUpdates == nil {
		d.writeDaemonStatus(ctx, update)
		return
	}
	d.enqueueDaemonStatusUpdate(ctx, update)
}

func (d *Daemon) enqueueDaemonStatusUpdate(ctx context.Context, update heartbeatStatusUpdate) {
	select {
	case d.statusUpdates <- update:
		return
	case <-ctx.Done():
		return
	default:
	}

	select {
	case <-d.statusUpdates:
	default:
	}

	select {
	case d.statusUpdates <- update:
	case <-ctx.Done():
	default:
	}
}

func (d *Daemon) runDaemonStatusPublisher(ctx context.Context, done chan struct{}) {
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			return
		case update := <-d.statusUpdates:
			d.writeDaemonStatus(ctx, update)
		}
	}
}

func (d *Daemon) writeDaemonStatus(ctx context.Context, update heartbeatStatusUpdate) {
	writeCtx := ctx
	cancel := func() {}
	if d.statusWriteTimeout > 0 {
		writeCtx, cancel = context.WithTimeout(ctx, d.statusWriteTimeout)
	}
	defer cancel()

	if err := d.statusWriter.Update(writeCtx, update.status, update.heartbeatAt); err != nil && ctx.Err() == nil && d.logf != nil {
		d.logf("daemon status update failed: %v", err)
	}
}

func (d *Daemon) lastHeartbeatTime() time.Time {
	heartbeatAt := d.lastHeartbeat.Load()
	if heartbeatAt <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(heartbeatAt).UTC()
}

func (d *Daemon) logWatchdogWarning(staleFor time.Duration, heartbeatAt time.Time) {
	if d.logf == nil {
		return
	}
	d.logf(
		"daemon poll loop heartbeat stale: age=%s threshold=%s last_heartbeat_at=%s",
		formatHeartbeatAge(staleFor),
		formatHeartbeatAge(watchdogWarningMultiplier*d.captureInterval),
		heartbeatAt.UTC().Format(time.RFC3339),
	)
}

func formatHeartbeatAge(age time.Duration) string {
	if age < time.Second {
		return "0s"
	}
	return age.Round(time.Second).String()
}
