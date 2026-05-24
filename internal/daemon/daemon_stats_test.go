package daemon

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type daemonStatsTestMessage struct {
	Goroutines          int `json:"goroutines"`
	InFlightTasks       int `json:"in_flight_tasks"`
	PostgresConnections struct {
		OpenConnections int `json:"open_connections"`
		InUse           int `json:"in_use"`
		Idle            int `json:"idle"`
	} `json:"postgres_connections"`
	BreakerStates struct {
		Open   int `json:"open"`
		Closed int `json:"closed"`
	} `json:"breaker_states"`
	ReconcileFindings   int            `json:"reconcile_findings"`
	WorkerRows          int            `json:"worker_rows"`
	PoolEntriesByStatus map[string]int `json:"pool_entries_by_status"`
}

type daemonStatsTestState struct {
	StateStore
	openConnections int
	inUse           int
	idle            int
}

func (s daemonStatsTestState) PostgresConnectionStats() (int, int, int) {
	return s.openConnections, s.inUse, s.idle
}

type daemonStatsTestPool struct {
	Pool
	entries []Clone
	err     error
}

func (p daemonStatsTestPool) PoolEntries(ctx context.Context, project string) ([]Clone, error) {
	if p.err != nil {
		return nil, p.err
	}
	entries := make([]Clone, len(p.entries))
	copy(entries, p.entries)
	return entries, nil
}

func TestDaemonStatsEmitsOnStatsTicker(t *testing.T) {
	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	statsTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker, statsTicker)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.StatsInterval = 5 * time.Minute
	})
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := d.Stop(ctx); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	}()

	if got := deps.events.countType("daemon.stats"); got != 0 {
		t.Fatalf("daemon.stats events before ticker = %d, want 0", got)
	}

	deps.clock.Advance(5 * time.Minute)
	statsTicker.tick(deps.clock.Now())
	waitFor(t, "first stats event", func() bool {
		return deps.events.countType("daemon.stats") == 1
	})

	deps.clock.Advance(5 * time.Minute)
	statsTicker.tick(deps.clock.Now())
	waitFor(t, "second stats event", func() bool {
		return deps.events.countType("daemon.stats") == 2
	})
}

func TestDaemonStatsMessageIncludesPopulatedFields(t *testing.T) {
	deps := newTestDeps(t)
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-1872-A",
		Status:       TaskStatusActive,
		WorkerID:     "worker-01",
		PaneID:       "pane-01",
		PaneName:     "worker-01",
		ClonePath:    "/tmp/project/.orca/pool/clone-01",
		AgentProfile: "codex",
	})
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-1872-B",
		Status:       TaskStatusStarting,
		WorkerID:     "worker-02",
		PaneID:       "pane-02",
		PaneName:     "worker-02",
		ClonePath:    "/tmp/project/.orca/pool/clone-02",
		AgentProfile: "codex",
	})
	deps.state.putTaskForTest(Task{
		Project: "/tmp/project",
		Issue:   "LAB-1872-C",
		Status:  TaskStatusDone,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-01",
		PaneID:       "pane-01",
		PaneName:     "worker-01",
		Issue:        "LAB-1872-A",
		ClonePath:    "/tmp/project/.orca/pool/clone-01",
		AgentProfile: "codex",
	}); err != nil {
		t.Fatalf("PutWorker(worker-01) error = %v", err)
	}
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-02",
		PaneID:       "pane-02",
		PaneName:     "worker-02",
		Issue:        "LAB-1872-B",
		ClonePath:    "/tmp/project/.orca/pool/clone-02",
		AgentProfile: "codex",
	}); err != nil {
		t.Fatalf("PutWorker(worker-02) error = %v", err)
	}
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-03",
		PaneID:       "pane-03",
		PaneName:     "worker-03",
		AgentProfile: "codex",
	}); err != nil {
		t.Fatalf("PutWorker(worker-03) error = %v", err)
	}

	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	statsTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker, statsTicker)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.StatsInterval = 5 * time.Minute
		opts.State = daemonStatsTestState{
			StateStore:      deps.state,
			openConnections: 7,
			inUse:           3,
			idle:            4,
		}
		opts.Pool = daemonStatsTestPool{
			Pool: deps.pool,
			entries: []Clone{
				{Name: "clone-01", Status: "free"},
				{Name: "clone-02", Status: "occupied"},
				{Name: "clone-03", Status: "occupied"},
				{Name: "clone-04", Status: "quarantined"},
			},
		}
	})
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := d.Stop(ctx); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	}()

	deps.clock.Advance(5 * time.Minute)
	statsTicker.tick(deps.clock.Now())
	stats := lastDaemonStatsMessage(t, deps)

	if stats.Goroutines <= 0 {
		t.Fatalf("goroutines = %d, want positive", stats.Goroutines)
	}
	if got, want := stats.InFlightTasks, 2; got != want {
		t.Fatalf("in_flight_tasks = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.OpenConnections, 7; got != want {
		t.Fatalf("postgres_connections.open_connections = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.InUse, 3; got != want {
		t.Fatalf("postgres_connections.in_use = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.Idle, 4; got != want {
		t.Fatalf("postgres_connections.idle = %d, want %d", got, want)
	}
	if got, want := stats.BreakerStates.Open, 0; got != want {
		t.Fatalf("breaker_states.open = %d, want %d", got, want)
	}
	if got, want := stats.BreakerStates.Closed, 1; got != want {
		t.Fatalf("breaker_states.closed = %d, want %d", got, want)
	}
	if got, want := stats.ReconcileFindings, 0; got != want {
		t.Fatalf("reconcile_findings = %d, want %d", got, want)
	}
	if got, want := stats.WorkerRows, 3; got != want {
		t.Fatalf("worker_rows = %d, want %d", got, want)
	}
	if got, want := stats.PoolEntriesByStatus["free"], 1; got != want {
		t.Fatalf("pool_entries_by_status.free = %d, want %d", got, want)
	}
	if got, want := stats.PoolEntriesByStatus["occupied"], 2; got != want {
		t.Fatalf("pool_entries_by_status.occupied = %d, want %d", got, want)
	}
	if got, want := stats.PoolEntriesByStatus["quarantined"], 1; got != want {
		t.Fatalf("pool_entries_by_status.quarantined = %d, want %d", got, want)
	}
}

func TestDaemonStatsHandlesMissingOptionalSources(t *testing.T) {
	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	statsTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker, statsTicker)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.StatsInterval = 5 * time.Minute
	})
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := d.Stop(ctx); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	}()

	deps.clock.Advance(5 * time.Minute)
	statsTicker.tick(deps.clock.Now())
	stats := lastDaemonStatsMessage(t, deps)

	if got, want := stats.PostgresConnections.OpenConnections, 0; got != want {
		t.Fatalf("postgres_connections.open_connections = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.InUse, 0; got != want {
		t.Fatalf("postgres_connections.in_use = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.Idle, 0; got != want {
		t.Fatalf("postgres_connections.idle = %d, want %d", got, want)
	}
	if stats.PoolEntriesByStatus == nil {
		t.Fatal("pool_entries_by_status = nil, want empty object")
	}
	if got := len(stats.PoolEntriesByStatus); got != 0 {
		t.Fatalf("pool_entries_by_status length = %d, want 0", got)
	}
}

func lastDaemonStatsMessage(t *testing.T, deps *testDeps) daemonStatsTestMessage {
	t.Helper()

	waitFor(t, "stats event", func() bool {
		return deps.events.countType("daemon.stats") > 0
	})
	event, ok := deps.events.lastEventOfType("daemon.stats")
	if !ok {
		t.Fatal("daemon.stats event missing")
	}
	var stats daemonStatsTestMessage
	if err := json.Unmarshal([]byte(event.Message), &stats); err != nil {
		t.Fatalf("daemon.stats message is not valid JSON: %v\nmessage: %s", err, event.Message)
	}
	return stats
}
