package daemon

import (
	"os"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

const daemonHeartbeatStaleReason = "heartbeat stale but daemon socket responding"

func (d *Daemon) projectStatusWithLiveDaemon(status state.ProjectStatus) state.ProjectStatus {
	if d == nil {
		return status
	}

	heartbeatAt := d.lastHeartbeatTime()
	if heartbeatAt.IsZero() {
		return status
	}

	daemonStatus := status.Daemon
	if daemonStatus == nil {
		daemonStatus = &state.DaemonStatus{}
	} else {
		copy := *daemonStatus
		daemonStatus = &copy
	}
	if daemonStatus.Session == "" {
		daemonStatus.Session = d.session
	}
	if daemonStatus.PID == 0 {
		daemonStatus.PID = os.Getpid()
	}
	if daemonStatus.UpdatedAt.IsZero() || heartbeatAt.After(daemonStatus.UpdatedAt) {
		daemonStatus.UpdatedAt = heartbeatAt
	}

	daemonStatus.Status = daemonStatusRunning
	daemonStatus.Reason = ""
	if d.liveHeartbeatStale(heartbeatAt) {
		daemonStatus.Status = daemonStatusDegraded
		daemonStatus.Reason = daemonHeartbeatStaleReason
	}

	status.Daemon = daemonStatus
	return status
}

func (d *Daemon) liveHeartbeatStale(heartbeatAt time.Time) bool {
	now := time.Now().UTC()
	if d.now != nil {
		now = d.now()
	}

	interval := d.captureInterval
	if interval <= 0 {
		interval = defaultCaptureInterval
	}
	return now.Sub(heartbeatAt) > watchdogWarningMultiplier*interval
}
