package daemon

import (
	"context"
	"time"
)

const (
	watchdogWarningMultiplier   = 2
	watchdogUnhealthyMultiplier = 5
)

type daemonStatusWriter interface {
	Update(ctx context.Context, status string, heartbeatAt time.Time) error
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
	if err := d.statusWriter.Update(ctx, status, heartbeatAt); err != nil && d.logf != nil {
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
