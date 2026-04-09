package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDaemonPollLoopRecordsHeartbeatAndRunningStatus(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)

	statusWriter := &fakeDaemonStatusWriter{}
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DaemonStatusWriter = statusWriter
	})

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.clock.Advance(5 * time.Second)
	captureTicker.tick(deps.clock.Now())

	waitFor(t, "heartbeat update", func() bool {
		update, ok := statusWriter.lastUpdate()
		return ok && update.Status == "running"
	})

	update, ok := statusWriter.lastUpdate()
	if !ok {
		t.Fatal("lastUpdate() = ok false, want heartbeat update")
	}
	if got, want := d.lastHeartbeat.Load(), deps.clock.Now().UnixMilli(); got != want {
		t.Fatalf("lastHeartbeat = %d, want %d", got, want)
	}
	if got, want := update.HeartbeatAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("heartbeatAt = %v, want %v", got, want)
	}
}

func TestDaemonWatchdogWarnsAndMarksUnhealthyAfterStaleHeartbeat(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	watchdogTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	deps.watchdogTickers.enqueue(watchdogTicker)

	statusWriter := &fakeDaemonStatusWriter{}
	logs := &fakeLogSink{}
	startedAt := deps.clock.Now()
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DaemonStatusWriter = statusWriter
		opts.Logf = logs.Printf
	})

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.clock.Advance(11 * time.Second)
	watchdogTicker.tick(deps.clock.Now())
	waitFor(t, "watchdog warning", func() bool {
		return len(logs.messages()) == 1
	})

	if got := logs.messages()[0]; !strings.Contains(got, "daemon poll loop heartbeat stale") {
		t.Fatalf("warning log = %q, want stale heartbeat warning", got)
	}
	if _, ok := statusWriter.lastUpdate(); ok {
		t.Fatal("statusWriter updated before unhealthy threshold, want no status update")
	}

	deps.clock.Advance(15 * time.Second)
	watchdogTicker.tick(deps.clock.Now())
	waitFor(t, "watchdog unhealthy update", func() bool {
		update, ok := statusWriter.lastUpdate()
		return ok && update.Status == "unhealthy"
	})

	update, ok := statusWriter.lastUpdate()
	if !ok {
		t.Fatal("lastUpdate() = ok false, want unhealthy update")
	}
	if got, want := update.Status, "unhealthy"; got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
	if got, want := update.HeartbeatAt, startedAt; !got.Equal(want) {
		t.Fatalf("heartbeatAt = %v, want %v", got, want)
	}
	if got, want := len(logs.messages()), 1; got != want {
		t.Fatalf("warning count = %d, want %d", got, want)
	}
}

type fakeDaemonStatusWriter struct {
	mu      sync.Mutex
	updates []daemonStatusUpdate
}

type daemonStatusUpdate struct {
	Status      string
	HeartbeatAt time.Time
}

func (f *fakeDaemonStatusWriter) Update(ctx context.Context, status string, heartbeatAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, daemonStatusUpdate{
		Status:      status,
		HeartbeatAt: heartbeatAt,
	})
	return nil
}

func (f *fakeDaemonStatusWriter) lastUpdate() (daemonStatusUpdate, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.updates) == 0 {
		return daemonStatusUpdate{}, false
	}
	return f.updates[len(f.updates)-1], true
}

type fakeLogSink struct {
	mu   sync.Mutex
	logs []string
}

func (f *fakeLogSink) Printf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}

func (f *fakeLogSink) messages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.logs))
	copy(out, f.logs)
	return out
}
