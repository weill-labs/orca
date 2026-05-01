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

func TestDaemonStatusUpdatesUseBoundedContext(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)

	statusWriter := &deadlineCheckingDaemonStatusWriter{seen: make(chan bool, 1)}
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

	select {
	case hasDeadline := <-statusWriter.seen:
		if !hasDeadline {
			t.Fatal("daemon status update context has no deadline")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for daemon status update")
	}
}

func TestDaemonPollLoopKeepsHeartbeatingWhenStatusWriterStalls(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)

	statusWriter := newStallingDaemonStatusWriter()
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
	firstHeartbeatAt := deps.clock.Now()
	captureTicker.tick(firstHeartbeatAt)
	statusWriter.waitForStall(t)

	deps.clock.Advance(5 * time.Second)
	secondHeartbeatAt := deps.clock.Now()
	captureTicker.tick(secondHeartbeatAt)

	waitFor(t, "heartbeat after stalled status update", func() bool {
		return d.lastHeartbeat.Load() == secondHeartbeatAt.UnixMilli()
	})
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
		opts.NewWatchdogTicker = deps.watchdogTickers.NewTicker
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

func TestDaemonWatchdogRecoveryWritesRunningHeartbeat(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	watchdogTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	deps.watchdogTickers.enqueue(watchdogTicker)

	statusWriter := &fakeDaemonStatusWriter{}
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DaemonStatusWriter = statusWriter
		opts.NewWatchdogTicker = deps.watchdogTickers.NewTicker
	})

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	startedAt := deps.clock.Now()
	deps.clock.Advance(26 * time.Second)
	watchdogTicker.tick(deps.clock.Now())
	waitFor(t, "watchdog unhealthy update", func() bool {
		update, ok := statusWriter.lastUpdate()
		return ok && update.Status == daemonStatusUnhealthy
	})

	unhealthyUpdate, ok := statusWriter.lastUpdate()
	if !ok {
		t.Fatal("lastUpdate() = ok false, want unhealthy update")
	}
	if got, want := unhealthyUpdate.HeartbeatAt, startedAt; !got.Equal(want) {
		t.Fatalf("heartbeatAt = %v, want %v", got, want)
	}

	deps.clock.Advance(4 * time.Second)
	captureTicker.tick(deps.clock.Now())
	waitFor(t, "running heartbeat update", func() bool {
		update, ok := statusWriter.lastUpdate()
		return ok && update.Status == daemonStatusRunning
	})

	runningUpdate, ok := statusWriter.lastUpdate()
	if !ok {
		t.Fatal("lastUpdate() = ok false, want running update")
	}
	if got, want := runningUpdate.HeartbeatAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("heartbeatAt = %v, want %v", got, want)
	}
	if got, want := d.lastHeartbeat.Load(), deps.clock.Now().UnixMilli(); got != want {
		t.Fatalf("lastHeartbeat = %d, want %d", got, want)
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

type deadlineCheckingDaemonStatusWriter struct {
	seen chan bool
}

func (f *deadlineCheckingDaemonStatusWriter) Update(ctx context.Context, status string, heartbeatAt time.Time) error {
	_, hasDeadline := ctx.Deadline()
	select {
	case f.seen <- hasDeadline:
	default:
	}
	return nil
}

type stallingDaemonStatusWriter struct {
	started chan struct{}
	once    sync.Once
}

func newStallingDaemonStatusWriter() *stallingDaemonStatusWriter {
	return &stallingDaemonStatusWriter{started: make(chan struct{})}
}

func (f *stallingDaemonStatusWriter) Update(ctx context.Context, status string, heartbeatAt time.Time) error {
	f.once.Do(func() {
		close(f.started)
	})
	<-ctx.Done()
	return ctx.Err()
}

func (f *stallingDaemonStatusWriter) waitForStall(t *testing.T) {
	t.Helper()

	select {
	case <-f.started:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for stalled daemon status update")
	}
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
