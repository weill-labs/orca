package daemon

import (
	"os"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestProjectStatusWithLiveDaemonUsesLiveFreshHeartbeat(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)
	d := &Daemon{
		session:         "orca",
		captureInterval: 5 * time.Second,
		now:             func() time.Time { return now },
	}
	d.lastHeartbeat.Store(now.UnixMilli())

	status := d.projectStatusWithLiveDaemon(state.ProjectStatus{Project: "/repo"})

	if status.Daemon == nil {
		t.Fatal("Daemon = nil, want live daemon status")
	}
	if got, want := status.Daemon.Status, daemonStatusRunning; got != want {
		t.Fatalf("daemon status = %q, want %q", got, want)
	}
	if got, want := status.Daemon.Session, "orca"; got != want {
		t.Fatalf("daemon session = %q, want %q", got, want)
	}
	if got, want := status.Daemon.PID, os.Getpid(); got != want {
		t.Fatalf("daemon pid = %d, want %d", got, want)
	}
	if got, want := status.Daemon.UpdatedAt, now; !got.Equal(want) {
		t.Fatalf("daemon updated_at = %v, want %v", got, want)
	}
	if got := status.Daemon.Reason; got != "" {
		t.Fatalf("daemon reason = %q, want empty", got)
	}
}

func TestProjectStatusWithLiveDaemonKeepsNewerStoredHeartbeat(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)
	d := &Daemon{
		captureInterval: 5 * time.Second,
		now:             func() time.Time { return now },
	}
	d.lastHeartbeat.Store(now.Add(-time.Second).UnixMilli())
	storedAt := now

	status := d.projectStatusWithLiveDaemon(state.ProjectStatus{
		Daemon: &state.DaemonStatus{
			Session:   "stored",
			PID:       42,
			Status:    daemonStatusUnhealthy,
			UpdatedAt: storedAt,
		},
	})

	if got, want := status.Daemon.Status, daemonStatusRunning; got != want {
		t.Fatalf("daemon status = %q, want %q", got, want)
	}
	if got, want := status.Daemon.UpdatedAt, storedAt; !got.Equal(want) {
		t.Fatalf("daemon updated_at = %v, want stored timestamp %v", got, want)
	}
	if got, want := status.Daemon.Session, "stored"; got != want {
		t.Fatalf("daemon session = %q, want %q", got, want)
	}
	if got, want := status.Daemon.PID, 42; got != want {
		t.Fatalf("daemon pid = %d, want %d", got, want)
	}
}

func TestProjectStatusWithLiveDaemonSetsHeartbeatWhenNewerThanStored(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)
	d := &Daemon{
		captureInterval: 5 * time.Second,
		now:             func() time.Time { return now },
	}
	d.lastHeartbeat.Store(now.UnixMilli())

	status := d.projectStatusWithLiveDaemon(state.ProjectStatus{
		Daemon: &state.DaemonStatus{
			Status:    daemonStatusRunning,
			UpdatedAt: now.Add(-time.Second),
		},
	})

	if got, want := status.Daemon.UpdatedAt, now; !got.Equal(want) {
		t.Fatalf("daemon updated_at = %v, want live heartbeat %v", got, want)
	}
}

func TestProjectStatusWithLiveDaemonLeavesStatusWithoutHeartbeat(t *testing.T) {
	t.Parallel()

	stored := &state.DaemonStatus{Status: daemonStatusUnhealthy, PID: 42}
	status := (&Daemon{}).projectStatusWithLiveDaemon(state.ProjectStatus{Daemon: stored})

	if status.Daemon != stored {
		t.Fatalf("Daemon pointer changed without live heartbeat")
	}
}

func TestProjectStatusWithLiveDaemonHandlesNilReceiver(t *testing.T) {
	t.Parallel()

	stored := &state.DaemonStatus{Status: daemonStatusUnhealthy, PID: 42}
	var d *Daemon
	status := d.projectStatusWithLiveDaemon(state.ProjectStatus{Daemon: stored})

	if status.Daemon != stored {
		t.Fatalf("Daemon pointer changed for nil receiver")
	}
}

func TestLiveHeartbeatStaleUsesDefaultCaptureInterval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)
	d := &Daemon{now: func() time.Time { return now }}

	if !d.liveHeartbeatStale(now.Add(-11 * time.Second)) {
		t.Fatal("liveHeartbeatStale() = false, want stale after two default capture intervals")
	}
	if d.liveHeartbeatStale(now.Add(-9 * time.Second)) {
		t.Fatal("liveHeartbeatStale() = true, want fresh before two default capture intervals")
	}
}
